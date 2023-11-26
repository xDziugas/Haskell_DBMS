module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))

import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import InMemoryTables qualified
import DataFrame (DataFrame(..), Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import Lib2 (parseStatement, ParsedStatement(..), Condition(..), ValueExpr(..))
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)

--temp imp
import Data.List (findIndices, findIndex, transpose)
import Data.Maybe (fromMaybe, isNothing, catMaybes, listToMaybe, isJust)
import Text.Read (readMaybe)
import Data.Maybe (fromJust, mapMaybe)
import Control.Monad (foldM)




type TableName = String
type FileContent = String
type ErrorMessage = String

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- runExecuteIO $ Lib3.executeSql c 
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.ExecuteSelect dfs stmt next) = do
          let processedData = executeSelectOperation dfs stmt
          return $ next processedData

        runStep (Lib3.ExecuteUpdate df stmt next) = do
          let processedData = executeUpdateOperation df stmt
          return $ next processedData

        runStep (Lib3.ExecuteInsert df stmt next) = do
          let processedData = executeInsertOperation df stmt
          return $ next processedData

        runStep (Lib3.GetTime next) = getCurrentTime >>= return . next

        runStep (Lib3.LoadFile tableName next) = do
          let relativePath = Lib3.getPath tableName
          fileContent <- readFile relativePath
          return (next fileContent)

        runStep (Lib3.ParseStringOfFile fileContent next) = do
          let parseContent = Lib3.parseContentToDataFrame fileContent
          return (next parseContent)

        runStep (Lib3.SerializeDataFrameToYAML tableName df next) = do
          returnedDf <- Lib3.writeDataFrameToYAML tableName df
          return (next returnedDf)

        runStep (Lib3.CheckDataFrame df next) = do
          let validationResult = Lib1.validateDataFrame df
          return (next validationResult)




        -- Mock SELECT operation
        executeSelectMock :: [DataFrame] -> ParsedStatement -> Either ErrorMessage DataFrame
        executeSelectMock dataFrames stmt@Select{qeFrom = tableNames} = do
            let maybeDataFrames = map (`lookup` InMemoryTables.database) tableNames
            case sequence maybeDataFrames of
                Just dfs -> executeSelectOperation dfs stmt 
                Nothing -> Left "One or more tables not found in InMemoryTables.database"

        ------------------- Execute INSERT -------------------
        ------------------------------------------------------
        executeInsertOperation :: DataFrame -> ParsedStatement -> Either ErrorMessage DataFrame
        executeInsertOperation (DataFrame cols rows) (Insert tableName colNames values) =
            if length colNames == length values then
                let newRow = map parseValue values
                in Right $ DataFrame cols (rows ++ [newRow])
            else
                Left "Column names and values count mismatch"
        executeInsertOperation _ _ = Left "Invalid insert operation"

        ------------------- Execute UPDATE -------------------
        ------------------------------------------------------
        executeUpdateOperation :: DataFrame -> ParsedStatement -> Either ErrorMessage DataFrame
        executeUpdateOperation df (Update _ setConditions maybeWhere) =
            applyUpdates df setConditions maybeWhere
        executeUpdateOperation _ _ = Left "Invalid operation"

        applyUpdates :: DataFrame -> [Condition] -> Maybe [Condition] -> Either ErrorMessage DataFrame
        applyUpdates (DataFrame cols rows) setConditions maybeWhere =
            case traverse (updateRowIfMatched setConditions maybeWhere cols) rows of
                Right updatedRows -> Right $ DataFrame cols updatedRows
                Left errorMsg -> Left errorMsg

        updateRowIfMatched :: [Condition] -> Maybe [Condition] -> [Column] -> Row -> Either ErrorMessage Row
        updateRowIfMatched setConditions maybeWhere columns row =
            case rowMatchesWhere maybeWhere columns row of
                Right True -> applySetConditions columns setConditions row
                Right False -> Right row
                Left errMsg -> Left errMsg

        applySetConditions :: [Column] -> [Condition] -> Row -> Either ErrorMessage Row
        applySetConditions columns setConditions row =
            foldM (applyConditionToUpdateRow columns) row setConditions

        applyConditionToUpdateRow :: [Column] -> Row -> Condition -> Either ErrorMessage Row
        applyConditionToUpdateRow columns row (Equals columnName newValue) =
            case findIndex (\(Column name _) -> name == columnName) columns of
                Just colIndex -> Right $ replaceAtIndex colIndex (parseValue newValue) row
                Nothing -> Left $ "Column not found: " ++ columnName
        applyConditionToUpdateRow _ _ _ = Left "Syntax error"

        replaceAtIndex :: Int -> a -> [a] -> [a]
        replaceAtIndex i newVal list = take i list ++ [newVal] ++ drop (i + 1) list

        parseValue :: String -> Value
        parseValue valStr =
            case readMaybe valStr :: Maybe Integer of
                Just intVal -> IntegerValue intVal
                Nothing -> case readMaybe valStr :: Maybe Bool of
                    Just boolVal -> BoolValue boolVal
                    Nothing -> StringValue valStr

        rowMatchesWhere :: Maybe [Condition] -> [Column] -> Row -> Either ErrorMessage Bool
        rowMatchesWhere Nothing _ _ = Right True
        rowMatchesWhere (Just conditions) columns row = allM (conditionSatisfied row columns) conditions

        allM :: Monad m => (a -> m Bool) -> [a] -> m Bool
        allM f = foldM (\prev nxt -> if prev then f nxt else return False) True



        ------------------- Execute SELECT -------------------
        ------------------------------------------------------
        executeSelectOperation :: [DataFrame] -> ParsedStatement -> Either ErrorMessage DataFrame
        executeSelectOperation dataFrames stmt =
            case stmt of
                Select columnNames tableNames maybeConditions -> do
                    -- Join
                    let conditions = fromMaybe [] maybeConditions
                    let joinedDataFrame = if length dataFrames > 1 
                                          then joinDataFrames dataFrames conditions
                                          else head dataFrames
                    -- Apply conditions
                    filteredDataFrame <- applyConditions joinedDataFrame conditions
                    -- Select columns
                    projectColumns filteredDataFrame columnNames
                _ -> Left "Invalid statement type for selection operation"

        ------------------- Join frames -------------------
        joinDataFrames :: [DataFrame] -> [Condition] -> DataFrame
        joinDataFrames [] _ = DataFrame [] []
        joinDataFrames dfs conditions = foldl1 (joinTwoDataFramesOnCondition conditions) dfs

        joinTwoDataFramesOnCondition :: [Condition] -> DataFrame -> DataFrame -> DataFrame
        joinTwoDataFramesOnCondition conditions (DataFrame cols1 rows1) (DataFrame cols2 rows2) = DataFrame joinedColumns joinedRows
          where
            commonColumns = map (\(Equals colName1 colName2) -> (colName1, colName2)) conditions
            excludedCols = filter (\(Column name _) -> notElem name (map snd commonColumns)) cols2
            joinedColumns = cols1 ++ excludedCols
            joinedRows = [r1 ++ filterRowByColumns r2 cols2 | r1 <- rows1, r2 <- rows2, rowsMatchOnConditions r1 r2 conditions cols1 cols2]

            filterRowByColumns :: Row -> [Column] -> Row
            filterRowByColumns row allCols = 
                map fst . filter (\(_, col) -> elem col excludedCols) $ zip row allCols

            rowsMatchOnConditions :: Row -> Row -> [Condition] -> [Column] -> [Column] -> Bool
            rowsMatchOnConditions r1 r2 conds cols1 cols2 =
                all (conditionSatisfiedForJoin r1 r2 cols1 cols2) conds

            conditionSatisfiedForJoin :: Row -> Row -> [Column] -> [Column] -> Condition -> Bool
            conditionSatisfiedForJoin r1 r2 cols1 cols2 (Equals colName1 colName2) =
                let val1 = findValueInRow r1 colName1 cols1
                    val2 = findValueInRow r2 colName2 cols2
                in case (val1, val2) of
                    (Just v1, Just v2) -> v1 == v2
                    _ -> False

            findValueInRow :: Row -> String -> [Column] -> Maybe Value
            findValueInRow row colName cols = 
                let colIndex = findIndex (\(Column name _) -> name == colName) cols
                in if colIndex >= Just 0 && colIndex < Just (length row)
                    then Just (row !! fromJust colIndex)
                    else Nothing


        ------------------- Apply conditions -------------------
        applyConditions :: DataFrame -> [Condition] -> Either ErrorMessage DataFrame
        applyConditions (DataFrame cols rows) conds = do
            filteredRows <- traverse (rowSatisfiesAllConditions conds cols) rows
            return $ DataFrame cols (map fst $ filter snd $ zip rows filteredRows)

        rowSatisfiesAllConditions :: [Condition] -> [Column] -> Row -> Either ErrorMessage Bool
        rowSatisfiesAllConditions conditions columns row = do
            results <- traverse (conditionSatisfied row columns) conditions
            return $ all id results


        conditionSatisfied :: Row -> [Column] -> Condition -> Either ErrorMessage Bool
        conditionSatisfied row columns condition = case condition of
            Equals colName1 colName2 ->
                if isColumnName colName1 && isColumnName colName2 then
                    Right True
                else
                    checkCondition (==) colName1 colName2
            LessThan par1 par2 ->
                checkIntegerCondition (<) par1 par2
            GreaterThan par1 par2 ->
                checkIntegerCondition (>) par1 par2
            LessEqualThan par1 par2 ->
                checkIntegerCondition (<=) par1 par2
            GreaterEqualThan par1 par2 ->
                checkIntegerCondition (>=) par1 par2
          where
            checkCondition :: (Value -> Value -> Bool) -> String -> String -> Either ErrorMessage Bool
            checkCondition op colName1 colName2 = 
                case findValueInRow colName1 of
                    Just val1 -> 
                        let val2 = parseLiteral colName2
                        in Right (val1 `op` val2)
                    Nothing -> Left $ "First operand is not a column name: " ++ colName1

            parseLiteral :: String -> Value
            parseLiteral literal =
                case readMaybe literal :: Maybe Integer of
                    Just intVal -> IntegerValue intVal
                    Nothing -> case readMaybe literal :: Maybe Bool of
                        Just boolVal -> BoolValue boolVal
                        Nothing -> StringValue literal  -- String if not an integer or boolean

            findValueInRow :: String -> Maybe Value
            findValueInRow colName = do
                colIndex <- findIndex (\(Column name _) -> name == colName) columns
                if colIndex >= 0 && colIndex < length row
                    then Just (row !! colIndex)
                    else Nothing


            checkIntegerCondition :: (Integer -> Integer -> Bool) -> String -> String -> Either ErrorMessage Bool
            checkIntegerCondition op colName1 colName2 =
                case findValueInRow colName1 of
                    Just (IntegerValue val1) -> case readMaybe colName2 of
                        Just val2 -> Right (val1 `op` val2)
                        Nothing -> Left $ "Second operand is not an integer: " ++ colName2
                    _ -> Left $ "Non-integer comparison or first operand not a column name"

            -- To return only if its an Integer
            parseIntegerLiteral :: String -> Maybe Integer
            parseIntegerLiteral str = readMaybe str :: Maybe Integer

            isColumnName :: String -> Bool
            isColumnName name = any (\(Column colName _) -> colName == name) columns



        ------------------- Project cols -------------------
        projectColumns :: DataFrame -> [ValueExpr] -> Either ErrorMessage DataFrame
        projectColumns (DataFrame allCols allRows) valueExprs =
            if any isAggregate valueExprs && all isAggregate valueExprs
            then calculateAggregate allCols allRows valueExprs
            else let colIndices = concatMap (getColumnIndicesByName allCols) valueExprs
                in if not (null colIndices)
                    then Right $ DataFrame (map (allCols !!) colIndices) (map (projectRow colIndices) allRows)
                    else Left "One or more columns not found"

        isAggregate :: ValueExpr -> Bool
        isAggregate (AggMin _) = True
        isAggregate (AggAvg _) = True
        isAggregate _ = False

        calculateAggregate :: [Column] -> [[Value]] -> [ValueExpr] -> Either ErrorMessage DataFrame
        calculateAggregate allCols allRows valueExprs = 
            let aggResults = map (processAggregate allCols allRows) valueExprs
            in case sequence aggResults of
                Just results -> Right $ DataFrame [Column "aggregate" IntegerType] [results]
                Nothing -> Left "Invalid aggregate function"

        processAggregate :: [Column] -> [[Value]] -> ValueExpr -> Maybe Value
        processAggregate allCols allRows (AggMin colName) = Just $ applyMin $ extractColumnValues allCols allRows colName
        processAggregate allCols allRows (AggAvg colName) = Just $ applyAvg $ extractColumnValues allCols allRows colName
        processAggregate _ _ _ = Nothing

        extractColumnValues :: [Column] -> [[Value]] -> String -> [Value]
        extractColumnValues allCols allRows colName = 
            case findIndex (\(Column name _) -> name == colName) allCols of
                Just colIndex -> map (!! colIndex) allRows
                Nothing -> []

        applyMin :: [Value] -> Value
        applyMin [] = NullValue
        applyMin (x:xs) = myMin x xs

        myMin :: Value -> [Value] -> Value
        myMin currentMin [] = currentMin
        myMin (IntegerValue a) (IntegerValue b : rest) = myMin (IntegerValue (min a b)) rest
        myMin currentMin (_ : rest) = myMin currentMin rest

        applyAvg :: [Value] -> Value
        applyAvg [] = NullValue
        applyAvg values =
          let (sumValue, count) =
                foldl (\(sm, cnt) value ->
                  case value of
                    IntegerValue i -> (sm + i, cnt + 1)
                    _ -> (sm, cnt)
                ) (0, 0) values
          in if count > 0
            then IntegerValue (sumValue `div` count)
            else NullValue

        getColumnIndicesByName :: [Column] -> ValueExpr -> [Int]
        getColumnIndicesByName allCols (Name colName) =
            findIndices (\(Column name _) -> name == colName) allCols

        projectRow :: [Int] -> [Value] -> [Value]
        projectRow colIndices row = map (row !!) colIndices
