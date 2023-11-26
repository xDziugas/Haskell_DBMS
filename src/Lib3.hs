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
import Data.List (intercalate, findIndices, findIndex, partition)
import Data.Char (toLower)
import Lib2 (parseStatement, ParsedStatement(..), Condition(..), ValueExpr(..))
import InMemoryTables qualified
import Data.Maybe (fromMaybe, isNothing, fromJust)
import Text.Read (readMaybe)
import Control.Monad (foldM)
import Data.Time ( UTCTime, getCurrentTime, formatTime, defaultTimeLocale )

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
  = LoadFile TableName (FileContent -> next)  
  | GetTime (UTCTime -> next)
  | DisplayTime UTCTime (DataFrame -> next)
  | ParseStringOfFile FileContent ((Either ErrorMessage DataFrame) -> next)
  | SerializeDataFrameToYAML TableName DataFrame (DataFrame -> next)
  | CheckDataFrame DataFrame ((Either ErrorMessage DataFrame) -> next)
  | ExecuteSelect [DataFrame] ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteInsert DataFrame ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteUpdate DataFrame ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteDelete DataFrame ParsedStatement (Either ErrorMessage DataFrame -> next)
  deriving Functor

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

displayTime :: UTCTime -> Execution DataFrame
displayTime tm = liftF $ DisplayTime tm id

serializeDataFrameToYAML :: TableName -> DataFrame -> Execution DataFrame
serializeDataFrameToYAML tableName df = liftF $ SerializeDataFrameToYAML tableName df id

checkDataFrame :: DataFrame -> Execution (Either ErrorMessage DataFrame)
checkDataFrame df = liftF $ CheckDataFrame df id

parseFileContent :: FileContent -> Execution (Either ErrorMessage DataFrame)
parseFileContent fileContent = liftF $ ParseStringOfFile fileContent id

-------------------------executeSql--------------------------

executeSelect :: [DataFrame] -> ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeSelect dfs stmt = liftF $ ExecuteSelect dfs stmt id

executeInsert :: DataFrame -> ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeInsert df stmt = liftF $ ExecuteInsert df stmt id

executeUpdate :: DataFrame -> ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeUpdate df stmt = liftF $ ExecuteUpdate df stmt id

executeDelete :: DataFrame -> ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeDelete df stmt = liftF $ ExecuteDelete df stmt id

-- Constructs table loading steps
loadAndParseMultipleTables :: [TableName] -> Execution (Either ErrorMessage [DataFrame])
loadAndParseMultipleTables tables = chain tables []
  where
    chain :: [TableName] -> [DataFrame] -> Execution (Either ErrorMessage [DataFrame])
    chain [] dfs = return $ Right dfs
    chain (t:ts) dfs = loadFile t >>= \fileContent -> 
                    parseFileContent fileContent >>= \eitherDf ->
                    case eitherDf of
                      Right df -> 
                          checkDataFrame df >>= \validationResult ->
                          case validationResult of
                              Right validatedDf -> chain ts (dfs ++ [validatedDf])
                              Left errMsg -> return $ Left errMsg
                      Left errMsg -> return $ Left errMsg

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
    Right stmt@(Select{qeFrom = tables}) -> 
        loadAndParseMultipleTables tables >>= \eitherDfs ->
        case eitherDfs of
            Right dfs -> executeSelect dfs stmt
            Left errMsg -> return $ Left errMsg

    Right stmt@(Insert tableName _ _) ->
        loadFile tableName >>= \fileContent -> 
        parseFileContent fileContent >>= \eitherDf ->
        case eitherDf of
            Right df -> 
                checkDataFrame df >>= \validationResult ->
                case validationResult of
                    Right validatedDf ->
                        executeInsert validatedDf stmt >>= \insertResult ->
                        case insertResult of
                            Right updatedDataFrame -> 
                                serializeDataFrameToYAML tableName updatedDataFrame >>= \serializedResult ->
                                return $ Right serializedResult
                            Left errMsg -> return $ Left errMsg
                    Left errMsg -> return $ Left errMsg
            Left errMsg -> return $ Left errMsg

    Right stmt@(Update tableName _ _) ->
        loadFile tableName >>= \fileContent -> 
        parseFileContent fileContent >>= \eitherDf ->
        case eitherDf of
            Right df -> 
                checkDataFrame df >>= \validationResult ->
                case validationResult of
                    Right validatedDf ->
                        executeUpdate validatedDf stmt >>= \updateResult ->
                        case updateResult of
                            Right updatedDf -> 
                                serializeDataFrameToYAML tableName updatedDf >>= \serializedResult ->
                                return $ Right serializedResult
                            Left errMsg -> return $ Left errMsg
                    Left errMsg -> return $ Left errMsg
            Left errMsg -> return $ Left errMsg

    Right stmt@(Delete tableName _) ->
        loadFile tableName >>= \fileContent -> 
        parseFileContent fileContent >>= \eitherDf ->
        case eitherDf of
            Right df -> 
                checkDataFrame df >>= \validationResult ->
                case validationResult of
                    Right validatedDf -> 
                        executeDelete validatedDf stmt >>= \deleteResult ->
                        return $ deleteResult
                    Left errMsg -> return $ Left errMsg
            Left errMsg -> return $ Left errMsg

    Right stmt@(Now) ->
        getTime >>= \time -> do
          df <- displayTime time
          return $ Right df

    Left errorMsg -> return $ Left errorMsg




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



------------------- Execute INSERT -------------------
------------------------------------------------------
executeInsertOperation :: DataFrame -> ParsedStatement -> Either ErrorMessage DataFrame
executeInsertOperation (DataFrame cols rows) (Insert tableName colNames values) =
    if length colNames == length values then
        let newRow = createRowWithDefaults cols colNames values
        in Right $ DataFrame cols (rows ++ [newRow])
    else
        Left "Column names and values count mismatch"
executeInsertOperation _ _ = Left "Invalid insert operation"

createRowWithDefaults :: [Column] -> [String] -> [String] -> Row
createRowWithDefaults allCols insertCols values = 
    let colValuePairs = zip insertCols (map parseValue values)
    in map (findOrDefault colValuePairs) allCols

findOrDefault :: [(String, Value)] -> Column -> Value
findOrDefault colValuePairs (Column colName _) = 
    fromMaybe NullValue (lookup colName colValuePairs)


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
            let allColumns = concatMap (\(DataFrame cols _) -> cols) dataFrames
            let (joinConditions, filterConditions) = splitConditions allColumns $ fromMaybe [] maybeConditions

            -- join
            let joinedDataFrame = if length dataFrames > 1 
                                  then joinDataFrames dataFrames joinConditions
                                  else head dataFrames
            -- apply conditions
            filteredDataFrame <- applyConditions joinedDataFrame filterConditions

            -- select columns
            projectColumns filteredDataFrame columnNames
        _ -> Left "Invalid statement type for selection operation"
    where
        splitConditions :: [Column] -> [Condition] -> ([Condition], [Condition])
        splitConditions allCols conditions = partition (isJoinCondition allCols) conditions
        
        isJoinCondition :: [Column] -> Condition -> Bool
        isJoinCondition allCols (Equals colName1 colName2) = isColumnName allCols colName1 && isColumnName allCols colName2
        isJoinCondition _ _ = False
                
        isColumnName :: [Column] -> String -> Bool
        isColumnName allCols name = any (\(Column colName _) -> colName == name) allCols



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




-------------Test Interpreter----------------


runExecuteIOTest :: Execution r -> IO r
runExecuteIOTest (Pure r) = return r
runExecuteIOTest (Free step) = do
    next <- runStep step
    runExecuteIOTest next
    where
        runStep :: ExecutionAlgebra a -> IO a
        runStep (ExecuteSelect dfs stmt next) = do
          let processedData = executeSelectOperation dfs stmt
          return $ next processedData

        runStep (ExecuteUpdate df stmt next) = do
          let processedData = executeUpdateOperation df stmt
          return $ next processedData

        runStep (ExecuteInsert df stmt next) = do
          let processedData = executeInsertOperation df stmt
          return $ next processedData

        runStep (GetTime next) = do
          -- Return frozen time for testing
          let testTime = read "2000-01-01 12:00:00 UTC" :: UTCTime
          return $ next testTime

        runStep (DisplayTime time next) = do
          let timestr = formatTime defaultTimeLocale "%F %T" time
          let df = DataFrame [Column "current_time" StringType] [[StringValue timestr]]
          return $ next df

        runStep (LoadFile tableName next) = return (next tableName)
            -- Return the name for testing

        runStep (ParseStringOfFile tableName next) = do
          -- Return data from InMemoryTables
          let maybeDataFrame = lookup tableName InMemoryTables.database
          case maybeDataFrame of
            Just df -> return (next (Right df))
            Nothing -> return (next (Left "Table not found in InMemoryTables"))

        runStep (SerializeDataFrameToYAML _ df next) = do
          -- Skip for testing
          return (next df)

        runStep (CheckDataFrame df next) = do
          let validationResult = Lib1.validateDataFrame df
          return (next validationResult)