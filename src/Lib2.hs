{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    ParsedStatement (..),
    executeStatement
  )
where

import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row)
import InMemoryTables (TableName, database)
import Data.Char (toLower, isAlphaNum)
import Lib1 (renderDataFrameAsTable)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data ColumnName = ColumnName String (Maybe Condition)
  deriving (Show, Eq)

data Condition
  = Equals ColumnName String
  | LessThan ColumnName String
  | GreaterThan ColumnName String
  | LessEqualThan ColumnName String
  | GreaterEqualThan ColumnName String
  | Min
  | Avg
  deriving (Show, Eq)

data ParsedStatement
  = ShowTables
  | ShowTable String
  | Select [ColumnName] TableName [Condition]
  deriving (Show, Eq)

-- Parse a string into a ParsedStatement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input = case words input of
  [] -> Left "No statement was given"
  (firstWord : rest) -> case map toLower firstWord of
    "show" -> parseShowStatement rest
    "select" -> parseSelectStatement rest
    _ -> Left "Invalid statement: the statement should start with the keyword SHOW or SELECT"

parseShowStatement :: [String] -> Either ErrorMessage ParsedStatement
parseShowStatement [] = Left "Invalid show statement: after SHOW keyword no keyword TABLE or TABLES was found"
parseShowStatement (secondWord : remaining) = case map toLower secondWord of
  "tables" -> Right ShowTables
  "table" -> case remaining of
    [tableName] -> Right (ShowTable tableName)
    _ -> Left "Invalid show statement: there can only be one table"
  _ -> Left "Invalid show statement: after SHOW keyword no keyword TABLE or TABLES was found"


parseSelectStatement :: [String] -> Either ErrorMessage ParsedStatement
parseSelectStatement input =
  let (columns, rest) = span (not . isFrom) input
      fromKeyword = map toLower (if null rest then "" else rest !! 0)
      whereKeyword = map toLower (rest !! 2)
      conditions = drop 3 rest
      tableName = rest !! 1
  in
  if fromKeyword == "from"
    then do
      parsedColumns <- parseColumns  (unwords columns)
      if length rest == 2
        then
          Right (Select parsedColumns tableName [])
      else if whereKeyword == "where" && (length rest) >= 6
        then do
          parsedConditions <- parseConditions conditions parsedColumns
          Right (Select parsedColumns tableName parsedConditions)
      else
        Left "Invalid select statement: the keyword WHERE is not writen in the appropriate position or the condition in the WHERE clause is not valid"
    else
      Left "Invalid select statement: the keyword FROM is not writen in the appropriate position"

isFrom :: String -> Bool
isFrom word = map toLower word == "from"








parseColumns :: String -> Either ErrorMessage [ColumnName]
parseColumns input = do
  let colNames = splitColNames input
  colNameStructures <- traverse constructColumnName (map words colNames)
  return colNameStructures

splitColNames :: String -> [String]
splitColNames input = customSplit ',' input

customSplit :: Char -> String -> [String]
customSplit _ [] = [""]
customSplit delimiter (x:xs)
    | x == delimiter = "" : rest
    | otherwise = (x : head rest) : tail rest
  where
    rest = customSplit delimiter xs

constructColumnName :: [String] -> Either ErrorMessage ColumnName
constructColumnName [col]
    | (containsOnlyLettersAndNumbers col) = Right (ColumnName col Nothing)
    | otherwise = Left "the column name contains symbols that are not allowed"
constructColumnName [agg, col]
    | lowerAgg == "min" && containsOnlyLettersAndNumbers col = Right (ColumnName col (Just Min))
    | lowerAgg == "avg" && containsOnlyLettersAndNumbers col = Right (ColumnName col (Just Avg))
    | otherwise = Left "the column name or aggregate function contains symbols that are not allowed"
  where
    lowerAgg = map toLower agg
constructColumnName _ = Left "Invalid column name structure"

containsOnlyLettersAndNumbers :: String -> Bool
containsOnlyLettersAndNumbers = all isAlphaNum






parseConditions :: [String] -> [ColumnName] -> Either ErrorMessage [Condition]
parseConditions [] columns = Right []
parseConditions input columns =
    case break (\x -> (map toLower x) == "and") input of
      (conditionTokens, andKeyword : rest) -> do
        condition <- parseToken conditionTokens columns
        conditions <- parseConditions rest columns
        return (condition : conditions)
      _ -> do
        condition <- parseToken input columns
        return [condition]

parseToken :: [String] -> [ColumnName] -> Either ErrorMessage Condition
parseToken (colName : op : value : []) parsedColumns
    | not (matchesAnyInList colName parsedColumns) && not (matchesAnyInList value parsedColumns) = Left "Invalid column name inside a condition"
    | (matchesAnyInList colName parsedColumns) && (matchesAnyInList value parsedColumns) = Left "Cant do operations with two columns"
    | otherwise =
        case findColumnsPosition colName value parsedColumns of
              (ColumnName name _, validVal) ->
                  case op of
                      "=" -> if colName == name
                             then Right (Equals (ColumnName name Nothing) validVal)
                             else Right (Equals (ColumnName name Nothing) validVal)
                      "<" -> if colName == name
                             then Right (LessThan (ColumnName name Nothing) validVal)
                             else Right (GreaterThan (ColumnName name Nothing) validVal)
                      ">" -> if colName == name
                             then Right (GreaterThan (ColumnName name Nothing) validVal)
                             else Right (LessThan (ColumnName name Nothing) validVal)
                      ">=" -> if colName == name
                              then Right (GreaterEqualThan (ColumnName name Nothing) validVal)
                              else Right (LessEqualThan (ColumnName name Nothing) validVal)
                      "<=" -> if colName == name
                              then Right (LessEqualThan (ColumnName name Nothing) validVal)
                              else Right (GreaterEqualThan (ColumnName name Nothing) validVal)
                      _ -> Left "Invalid operator inside a condition"
parseToken _ _ = Left "Invalid condition"

matchesAnyInList :: String -> [ColumnName] -> Bool
matchesAnyInList input = any (\(ColumnName name _) -> name == input)

findColumnsPosition :: String -> String -> [ColumnName] -> (ColumnName, String)
findColumnsPosition colName value parsedColumns
    | matchesAnyInList colName parsedColumns && not (matchesAnyInList value parsedColumns) = (ColumnName colName Nothing, value)
    | not (matchesAnyInList colName parsedColumns) && matchesAnyInList value parsedColumns = (ColumnName value Nothing, colName)



-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- Right (Select [ColumnName "col1" (Just Min)] "table" [GreaterThan (ColumnName "col1" Nothing) "2"])
-- Right (Select [ColumnName "id" Nothing] "employees" [])
test = Select [ColumnName "id" Nothing] "employees" [GreaterThan (ColumnName "id" Nothing) "1"]
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables =
  let tableNames = map fst database
      columns = [Column "Table Names" StringType]
      rows = map (\name -> [StringValue name]) tableNames
  in Right (DataFrame columns rows)


executeStatement (ShowTable tableName) =
  case lookup tableName database of
    Nothing -> Left $ "Table with name " ++ tableName ++ " not found"
    Just dataFrame -> Right dataFrame


executeStatement (Select columnNames tableName conditions) =
  case lookup tableName database of
    Just tableData -> do
      let withSelectedColumns = selectColumns tableData columnNames
          withFilteredColumns = filterRows withSelectedColumns conditions
      return withSelectedColumns
    Nothing -> Left $ "Table with name " ++ tableName ++ " not found"


selectColumns :: DataFrame -> [ColumnName] -> DataFrame
selectColumns (DataFrame dataColumns dataRows) parsedColumns =
  let selectedColumnIndexes = myMapMaybe (\(ColumnName colName _) -> getColumnIndex colName dataColumns) parsedColumns
      selectedColumns = [dataColumns !! idx | idx <- selectedColumnIndexes]
      selectedRows = map (\row -> [row !! idx | idx <- selectedColumnIndexes]) dataRows
  in  DataFrame selectedColumns selectedRows


filterRows :: DataFrame -> [Condition] -> DataFrame
filterRows (DataFrame dataColumns dataRows) conditions =
  let
    evaluateCondition :: Row -> Condition -> Bool
    evaluateCondition row (Equals (ColumnName colName _) value) =
      case getColumnIndex colName dataColumns of
        Just index -> case row !! index of
          StringValue s -> s == value
          IntegerValue i -> i == read value
          _ -> False
        Nothing -> False
    evaluateCondition row (LessThan (ColumnName colName _) value) =
      case getColumnIndex colName dataColumns of
        Just index -> case row !! index of
          IntegerValue i -> i < read value
          _ -> False
        Nothing -> False
    evaluateCondition row (GreaterThan (ColumnName colName _) value) =
      case getColumnIndex colName dataColumns of
        Just index -> case row !! index of
          IntegerValue i -> i > read value
          _ -> False
        Nothing -> False
    evaluateCondition row (LessEqualThan (ColumnName colName _) value) =
      case getColumnIndex colName dataColumns of
        Just index -> case row !! index of
          IntegerValue i -> i <= read value
          _ -> False
        Nothing -> False
    evaluateCondition row (GreaterEqualThan (ColumnName colName _) value) =
      case getColumnIndex colName dataColumns of
        Just index -> case row !! index of
          IntegerValue i -> i >= read value
          _ -> False
        Nothing -> False

    filteredRows = filter (\row -> all (evaluateCondition row) conditions) dataRows

  in DataFrame dataColumns filteredRows

getColumnIndex :: String -> [Column] -> Maybe Int
getColumnIndex targetName columns =
  let
    indexedColumns = zip (map (\(Column name _) -> name) columns) [0..]
  in
    lookup targetName indexedColumns

myMapMaybe :: (a -> Maybe b) -> [a] -> [b]
myMapMaybe _ []     = []
myMapMaybe f (x:xs) =
 let rs = myMapMaybe f xs in
 case f x of
  Nothing -> rs
  Just r  -> r:rs



-- data ColumnName = ColumnName String (Maybe Condition)
--   deriving (Show, Eq)-- data Condition
--   = Equals ColumnName String
--   | LessThan ColumnName String
--   | GreaterThan ColumnName String
--   | LessEqualThan ColumnName String
--   | GreaterEqualThan ColumnName String
--   | Min
--   | Avg
--   deriving (Show, Eq)

-- data ParsedStatement
--   = ShowTables
--   | ShowTable String
--   | Select [ColumnName] TableName [Condition]
--   deriving (Show, Eq)

-- module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

-- data ColumnType
--   = IntegerType
--   | StringType
--   | BoolType
--   deriving (Show, Eq)

-- data Column = Column String ColumnType
--   deriving (Show, Eq)

-- data Value
--   = IntegerValue Integer
--   | StringValue String
--   | BoolValue Bool
--   | NullValue
--   deriving (Show, Eq)

-- type Row = [Value]

-- data DataFrame = DataFrame [Column] [Row]
--   deriving (Show, Eq)
