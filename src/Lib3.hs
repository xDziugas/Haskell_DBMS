{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    parseContentToDataFrame,
    getPath,
    writeDataFrameToYAML,
    executeSelectOperation,
    executeUpdateOperation,
    executeInsertOperation,
    executeDeleteOperation,
    runExecuteIOTest
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame(..), Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import Data.Time ( UTCTime )
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row(..))
import Data.Yaml (decodeFileEither, FromJSON, parseJSON, withObject, (.:), (.:?), YamlException, ParseException)
import Control.Exception (try, IOException)
import qualified Data.Yaml as Y
import Data.Text (pack, unpack) 
import Data.Aeson.Key (fromString)
import Lib1 qualified
import qualified Data.ByteString.Char8 as BS
import Data.List (intercalate, findIndices, findIndex, partition, sortBy)
import Data.Char (toLower)
import Lib2 (parseStatement, ParsedStatement(..), Condition(..), ValueExpr(..), Order(..))
import InMemoryTables qualified
import Data.Maybe (fromMaybe, isNothing, fromJust)
import Text.Read (readMaybe)
import Control.Monad (foldM)
import Data.Time ( UTCTime, getCurrentTime, formatTime, defaultTimeLocale )
import qualified Lib1

instance FromJSON Table where
  parseJSON = withObject "Table" $ \v ->
    Table <$> v .: fromString "columns"
          <*> v .: fromString "rows"

instance FromJSON ColumnDef where
  parseJSON = withObject "ColumnDef" $ \v -> do
    name <- v .: fromString "name"
    dataType <- v .: fromString "dataType"
    return $ ColumnDef name dataType

data ColumnDef = ColumnDef
  { columnName :: String
  , columnType :: String
  } deriving (Show, Eq)

data Table = Table
  { columns     :: [ColumnDef]
  , rows        :: [[YamlValue]]
  } deriving (Show, Eq)

data YamlValue = YamlInt Integer | YamlString String | YamlBool Bool | YamlNull
  deriving (Show, Eq)


instance FromJSON YamlValue where
  parseJSON (Y.Number n) = pure $ YamlInt (round n)
  parseJSON (Y.String s) = pure $ YamlString (unpack s)
  parseJSON (Y.Bool b) = pure $ YamlBool b
  parseJSON Y.Null = pure YamlNull
  parseJSON _ = fail "Invalid value"




type TableName = String
type FileContent = String
type ErrorMessage = String

type Execution = Free ExecutionAlgebra


data ExecutionAlgebra next
  = LoadFile TableName ((Either ErrorMessage DataFrame) -> next)
  | GetTime (UTCTime -> next)
  | SaveFile TableName DataFrame ((Either ErrorMessage DataFrame) -> next)
  deriving Functor

loadFile :: TableName -> Execution (Either ErrorMessage DataFrame)
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

saveFile :: TableName -> DataFrame -> Execution (Either ErrorMessage DataFrame)
saveFile tableName df = liftF $ SaveFile tableName df id

-------------------------executeSql--------------------------

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
    Right stmt@(Select{qeFrom = tables}) -> 
        loadAndParseMultipleTables tables >>= \eitherDfs ->
            case eitherDfs of
                Right dfs -> 
                    getTime >>= \currentTime ->
                        case executeSelectOperation dfs stmt currentTime of
                            Right df -> 
                                case Lib1.validateDataFrame df of
                                    Right validDf -> return $ Right validDf
                                    Left errMsg -> return $ Left errMsg
                            Left errMsg -> return $ Left errMsg
                Left errMsg -> return $ Left errMsg

    Right stmt@(Insert tableName _ _) -> executeStatement stmt tableName executeInsertOperation

    Right stmt@(Update tableName _ _) -> executeStatement stmt tableName executeUpdateOperation

    Right stmt@(Delete tableName _) -> executeStatement stmt tableName executeDeleteOperation

    Left errorMsg -> return $ Left errorMsg

executeStatement :: ParsedStatement -> TableName -> (DataFrame -> ParsedStatement -> Either ErrorMessage DataFrame) -> Execution (Either ErrorMessage DataFrame)
executeStatement stmt tableName operation = do
    eitherDf <- loadFile tableName
    case eitherDf of
        Right df ->
            case Lib1.validateDataFrame df of
                Right validDf -> 
                    case operation validDf stmt of
                        Right updatedDf ->
                            case Lib1.validateDataFrame updatedDf of 
                                Right validUpdatedDf -> saveFile tableName validUpdatedDf >>= return
                                Left errMsg -> return $ Left errMsg
                        Left errMsg -> return $ Left errMsg
                Left errMsg -> return $ Left errMsg
        Left errMsg -> return $ Left errMsg

loadAndParseMultipleTables :: [TableName] -> Execution (Either ErrorMessage [DataFrame])
loadAndParseMultipleTables tables = chain tables []
  where
    chain [] dfs = return $ Right dfs
    chain (t:ts) dfs = do
        parsedDf <- loadFile t
        case parsedDf of
            Right df -> 
                case Lib1.validateDataFrame df of 
                    Right validDf -> chain ts (dfs ++ [validDf])
                    Left errMsg -> return $ Left errMsg
            Left errMsg -> return $ Left errMsg



-------------------------parseContent--------------------------

parseContentToDataFrame :: FileContent -> Either ErrorMessage DataFrame
parseContentToDataFrame fileContent = 
  case parseYAMLToTable fileContent of
    Nothing -> Left "Parsing failed"
    Just table -> Right $ tableToDataFrame table

parseYAMLToTable :: String -> Maybe Table
parseYAMLToTable = Y.decode . BS.pack

tableToDataFrame :: Table -> DataFrame
tableToDataFrame tbl = DataFrame (map colDefToColumn $ columns tbl) 
                                 (map (parseRow $ columns tbl) $ rows tbl)

colDefToColumn :: ColumnDef -> Column
colDefToColumn (ColumnDef name dataType) = Column name (parseDataType dataType)

parseDataType :: String -> ColumnType
parseDataType "Int" = IntegerType
parseDataType "Bool" = BoolType
parseDataType "String" = StringType

parseRow :: [ColumnDef] -> [YamlValue] -> Row
parseRow columnDefs yamlValues = zipWith parseValue columnDefs yamlValues
  where
    parseValue (ColumnDef _ "Int") (YamlInt i) = IntegerValue i
    parseValue (ColumnDef _ "String") (YamlString s) = StringValue s
    parseValue (ColumnDef _ "Bool") (YamlBool b) = BoolValue b
    parseValue _ YamlNull = NullValue
    parseValue _ _ = error "Type mismatch"







writeDataFrameToYAML :: String -> DataFrame -> IO DataFrame
writeDataFrameToYAML fileName df = do
  let yamlContent = serializeDataFrame df
  writeFile ("db/" ++ fileName ++ ".yaml") yamlContent
  return df


serializeDataFrame :: DataFrame -> String
serializeDataFrame (DataFrame columns rows) = 
  "columns:\n" ++ unlines (map serializeColumn columns) ++ 
  "rows:\n" ++ unlines (map serializeRow rows)

serializeColumn :: Column -> String
serializeColumn (Column name dataType) = 
  "- name: " ++ name ++ "\n  dataType: " ++ dataTypeToString dataType

dataTypeToString :: ColumnType -> String
dataTypeToString IntegerType = "Int"
dataTypeToString StringType  = "String"
dataTypeToString BoolType    = "Bool"

serializeRow :: Row -> String
serializeRow row = 
  "- [" ++ intercalate ", " (map serializeValue row) ++ "]"

serializeValue :: Value -> String
serializeValue (IntegerValue i) = show i
serializeValue (StringValue s)  = s 
serializeValue (BoolValue b)    = map toLower $ show b 
serializeValue NullValue        = "null"

getPath :: String -> String
getPath tableName = "db/" ++ tableName ++ ".yaml"


------------------- Execute DELETE -------------------
------------------------------------------------------

executeDeleteOperation :: DataFrame -> ParsedStatement -> Either ErrorMessage DataFrame
executeDeleteOperation (DataFrame cols rows) (Delete _ maybeWhere) =
    case maybeWhere of
        Just conditions ->
            let updatedRows = filter (not . rowMatchesConditions conditions cols) rows
            in Right $ DataFrame cols updatedRows
        Nothing -> Right $ DataFrame cols []  -- If no conditions are provided, all rows are removed
executeDeleteOperation _ _ = Left "Invalid delete operation"

rowMatchesConditions :: [Condition] -> [Column] -> Row -> Bool
rowMatchesConditions conditions columns row = 
    all isConditionMet conditions
    where
      isConditionMet cond = case conditionSatisfied row columns cond of
                                Right True -> True
                                _ -> False


findValueInRow :: Row -> String -> [Column] -> Maybe Value
findValueInRow row colName cols =
    case findIndex (\(Column name _) -> name == colName) cols of
        Just colIndex -> if colIndex >= 0 && colIndex < length row
                         then Just (row !! colIndex)
                         else Nothing
        Nothing -> Nothing

parseLiteral :: String -> Maybe Value
parseLiteral literal =
    case readMaybe literal :: Maybe Integer of
        Just intVal -> Just $ IntegerValue intVal
        Nothing -> case readMaybe literal :: Maybe Bool of
            Just boolVal -> Just $ BoolValue boolVal
            Nothing -> Just $ StringValue literal




------------------- Execute INSERT -------------------
------------------------------------------------------
-- Checks if all insert columns exist in the DataFrame's schema
validateInsertColumns :: [Column] -> [String] -> Either ErrorMessage ()
validateInsertColumns allCols insertCols =
    let allColNames = map (\(Column name _) -> name) allCols
    in if all (`elem` allColNames) insertCols
       then Right ()
       else Left "One or more specified columns do not exist in the table."

createRowWithDefaults :: [Column] -> [String] -> [String] -> Either ErrorMessage Row
createRowWithDefaults allCols insertCols values = 
    case validateInsertColumns allCols insertCols of
        Left errMsg -> Left errMsg
        Right _ -> 
            let colValuePairs = zip insertCols (map parseValue values)
            in traverse (findOrDefault colValuePairs) allCols

executeInsertOperation :: DataFrame -> ParsedStatement -> Either ErrorMessage DataFrame
executeInsertOperation (DataFrame cols rows) (Insert tableName colNames values) =
    case createRowWithDefaults cols colNames values of
        Right newRow -> Right $ DataFrame cols (rows ++ [newRow])
        Left errMsg -> Left errMsg
executeInsertOperation _ _ = Left "Invalid insert operation"


findOrDefault :: [(String, Value)] -> Column -> Either ErrorMessage Value
findOrDefault colValuePairs (Column colName colType) = 
    case lookup colName colValuePairs of
        Just val -> if isTypeMatch val colType then Right val else Left $ "Type mismatch for column: " ++ colName
        Nothing -> Right NullValue

isTypeMatch :: Value -> ColumnType -> Bool
isTypeMatch (IntegerValue _) IntegerType = True
isTypeMatch (StringValue _) StringType = True
isTypeMatch (BoolValue _) BoolType = True
isTypeMatch NullValue _ = True
isTypeMatch _ _ = False


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
executeSelectOperation :: [DataFrame] -> ParsedStatement -> UTCTime -> Either ErrorMessage DataFrame
executeSelectOperation dataFrames stmt currentTime = 
    case stmt of
        Select columnNames tableNames maybeConditions maybeOrderBy -> do
            let allColumns = concatMap (\(DataFrame cols _) -> cols) dataFrames
            let (joinConditions, filterConditions) = splitConditions allColumns $ fromMaybe [] maybeConditions

            -- join
            let joinedDataFrame = if length dataFrames > 1 
                                  then joinDataFrames dataFrames joinConditions
                                  else head dataFrames
            -- apply conditions
            filteredDataFrame <- applyConditions joinedDataFrame filterConditions

            -- select columns
            projectedDataFrame <- projectColumnsWithNow filteredDataFrame columnNames currentTime

            -- apply order by
            applyOrderBy projectedDataFrame maybeOrderBy
        _ -> Left "Invalid statement type for selection operation"
    where
        splitConditions :: [Column] -> [Condition] -> ([Condition], [Condition])
        splitConditions allCols conditions = partition (isJoinCondition allCols) conditions
        
        isJoinCondition :: [Column] -> Condition -> Bool
        isJoinCondition allCols (Equals colName1 colName2) = isColumnName allCols colName1 && isColumnName allCols colName2
        isJoinCondition _ _ = False
                
        isColumnName :: [Column] -> String -> Bool
        isColumnName allCols name = any (\(Column colName _) -> colName == name) allCols

        projectColumnsWithNow :: DataFrame -> [ValueExpr] -> UTCTime -> Either ErrorMessage DataFrame
        projectColumnsWithNow df valueExprs currentTime =
            if Now `elem` valueExprs then
                if length valueExprs == 1 then
                    Right $ DataFrame [Column "current_time" StringType] [[StringValue $ formatTime defaultTimeLocale "%F %T" currentTime]]
                else
                    case projectColumns df (filter (/= Now) valueExprs) of
                        Right (DataFrame columns rows) -> 
                            let nowColumn = Column "current_time" StringType
                                nowValue = StringValue $ show currentTime
                                updatedRows = map (++ [nowValue]) rows
                            in Right $ DataFrame (columns ++ [nowColumn]) updatedRows
                        Left errMsg -> Left errMsg
            else
                projectColumns df valueExprs



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

-- The GOAT function, i love it so mutch
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
    if all isAggregate valueExprs
        then calculateAggregate allCols allRows valueExprs
    else if any isAggregate valueExprs 
        then Left "Column names alongside aggregate functions not allowed"
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
    if colName == "*" 
    then [0 .. length allCols - 1]
    else findIndices (\(Column name _) -> name == colName) allCols

projectRow :: [Int] -> [Value] -> [Value]
projectRow colIndices row = map (row !!) colIndices

------------------- Order by -------------------
applyOrderBy :: DataFrame -> Maybe [Order] -> Either ErrorMessage DataFrame
applyOrderBy df Nothing = Right df
applyOrderBy df (Just orders) = orderBy df orders

orderBy :: DataFrame -> [Order] -> Either ErrorMessage DataFrame
orderBy (DataFrame cols rows) orders =
    case mapM (findColumnIndexAndOrder cols) orders of
        Right orderSpecs -> Right $ DataFrame cols (sortRows rows orderSpecs)
        Left errMsg -> Left errMsg

findColumnIndexAndOrder :: [Column] -> Order -> Either ErrorMessage (Int, Bool)
findColumnIndexAndOrder cols order =
    let (colName, isAsc) = case order of
                             Asc name -> (name, True)
                             Desc name -> (name, False)
    in case findIndex (\(Column name _) -> name == colName) cols of
         Just colIndex -> Right (colIndex, isAsc)
         Nothing -> Left $ "Column not found: " ++ colName

sortRows :: [[Value]] -> [(Int, Bool)] -> [[Value]]
sortRows rows orderSpecs =
    sortBy (compareRowsByOrderSpecs orderSpecs) rows

compareRowsByOrderSpecs :: [(Int, Bool)] -> [Value] -> [Value] -> Ordering
compareRowsByOrderSpecs [] _ _ = EQ
compareRowsByOrderSpecs ((i, isAsc):os) row1 row2 =
    let basicOrder = compareValue (row1 !! i) (row2 !! i)
        order = if isAsc then basicOrder else invertOrder basicOrder
    in case order of
        EQ -> compareRowsByOrderSpecs os row1 row2
        _ -> order

invertOrder :: Ordering -> Ordering
invertOrder EQ = EQ
invertOrder LT = GT
invertOrder GT = LT

compareValue :: Value -> Value -> Ordering
compareValue (IntegerValue x) (IntegerValue y) = compare x y
compareValue (StringValue x) (StringValue y) = compare x y
compareValue (BoolValue x) (BoolValue y) = compare x y
compareValue NullValue NullValue = EQ
compareValue NullValue _ = LT
compareValue _ NullValue = GT
compareValue _ _ = EQ  



-------------Test Interpreter----------------


runExecuteIOTest :: Execution r -> IO r
runExecuteIOTest (Pure r) = return r
runExecuteIOTest (Free step) = do
    next <- runStep step
    runExecuteIOTest next
    where
        runStep :: ExecutionAlgebra a -> IO a
        runStep (GetTime next) = do
          -- Return frozen time for testing
          let testTime = read "2000-01-01 12:00:00 UTC" :: UTCTime
          return $ next testTime

        runStep (LoadFile tableName next) = do
          let maybeDataFrame = lookup tableName InMemoryTables.database
          case maybeDataFrame of
            Just df -> return $ next (Right df)
            Nothing -> return $ next (Left "Table not found in InMemoryTables")

        runStep (SaveFile _ df next) = do
          return $ next (Right df)