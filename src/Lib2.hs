{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    ParsedStatement (..),
    executeStatement,
    ColumnName (..),
    Condition (..),
  )
where

import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row(..))
import InMemoryTables (TableName, database)
import Data.Char (toLower, isAlphaNum)
import Lib1 (renderDataFrameAsTable)
import Data.Maybe (isJust)


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
  "tables" -> case length remaining of
    0 -> Right ShowTables
    _ -> Left "Invalid show statement"
  "table" -> case remaining of
    [tableName] -> case lookup tableName database of
      Nothing -> Left "Invalid show statement: no table name like that exists"
      Just table -> Right (ShowTable tableName)
    _ -> Left "Invalid show statement: there can only be one table"
  _ -> Left "Invalid show statement: after SHOW keyword no keyword TABLE or TABLES was found"

parseSelectStatement :: [String] -> Either ErrorMessage ParsedStatement
parseSelectStatement input =
  let (columns, rest) = span (not . isFrom) input
      columnsUnworded = unwords (columns)
      fromKeyword = map toLower (if null rest then "" else rest !! 0)
      whereKeyword = map toLower (rest !! 2)
      conditions = drop 3 rest
      tableName = rest !! 1
   in if (fromKeyword == "from")
        then do
          existingColumns <- getAllColumns tableName
          parsedColumns <- (if (checkForAsterix (words columnsUnworded)) then return [ColumnName "*" Nothing] else parseColumns columnsUnworded existingColumns)
          initializer <- checkColumnConditions parsedColumns
          if length rest == 2
            then Right (Select parsedColumns tableName [])
            else
              if whereKeyword == "where" && (length rest) >= 6
                then do
                  parsedConditions <- parseConditions conditions existingColumns
                  Right (Select parsedColumns tableName parsedConditions)
                else Left "Invalid select statement: the keyword WHERE is not writen in the appropriate position or the condition in the WHERE clause is not valid"
        else Left "Invalid select statement: the keyword FROM is not writen in the appropriate position"

isFrom :: String -> Bool
isFrom word = map toLower word == "from"

getAllColumns :: String -> Either ErrorMessage [ColumnName]
getAllColumns tableName =
  case lookup tableName database of
    Nothing -> Left "no table found by that name"
    Just table -> Right $ (\(DataFrame columns _) -> map (\(Column name _) -> ColumnName name Nothing) columns) table

checkForAsterix :: [String] -> Bool
checkForAsterix ["*"] = True
checkForAsterix _ = False

checkColumnConditions :: [ColumnName] -> Either ErrorMessage [a]
checkColumnConditions colNames =
  case filter hasCondition colNames of
    [] -> Right []
    [singleCol] -> if length colNames == 1 then Right ([]) else Left "Invalid select statement: multiple columnNames, when there's an aggregate function used"
    _ -> Left "Invalid select statement: Multiple columns with aggregates"

hasCondition :: ColumnName -> Bool
hasCondition (ColumnName _ condition) = isJust condition

parseColumns :: String -> [ColumnName] -> Either ErrorMessage [ColumnName]
parseColumns input columns = do
  let colNames = splitColNames input
  colNameStructures <- traverse constructColumnName (map words colNames)
  let columnStrings = map (\(ColumnName name _) -> name) colNameStructures
  let hasErrors = any (\s -> not (matchesAnyInList s columns)) columnStrings
  if hasErrors
    then Left "Invalid select statement: the provided column names are not part of the given table"
    else Right colNameStructures

splitColNames :: String -> [String]
splitColNames input = customSplit ',' input

customSplit :: Char -> String -> [String]
customSplit _ [] = [""]
customSplit delimiter (x : xs)
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
            "=" ->
              if colName == name
                then Right (Equals (ColumnName name Nothing) validVal)
                else Right (Equals (ColumnName name Nothing) validVal)
            "<" ->
              if colName == name
                then Right (LessThan (ColumnName name Nothing) validVal)
                else Right (GreaterThan (ColumnName name Nothing) validVal)
            ">" ->
              if colName == name
                then Right (GreaterThan (ColumnName name Nothing) validVal)
                else Right (LessThan (ColumnName name Nothing) validVal)
            ">=" ->
              if colName == name
                then Right (GreaterEqualThan (ColumnName name Nothing) validVal)
                else Right (LessEqualThan (ColumnName name Nothing) validVal)
            "<=" ->
              if colName == name
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

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables =
  let tableNames = map fst database
      columns = [Column "Table Names" StringType]
      rows = map (\name -> [StringValue name]) tableNames
  in Right (DataFrame columns rows)


executeStatement (ShowTable tableName) =
  case lookup tableName database of
    Nothing -> Left $ "Table with name " ++ tableName ++ " was not found"
    Just dataFrame -> Right dataFrame


executeStatement (Select columnNames tableName conditions) =
  case lookup tableName database of
    Just tableData -> do
      let withFilteredColumns = filterRows tableData conditions
          withSelectedColumns = selectColumns withFilteredColumns columnNames
          withAgregates = handleAggregate withSelectedColumns columnNames
      return withAgregates
    Nothing -> Left $ "Table with name " ++ tableName ++ " was not found"

checkForStar :: [ColumnName] -> [Column] -> [ColumnName]
checkForStar columnNames columns = 
  case (\(ColumnName name _) -> name) $ head columnNames of
    "*" -> map (\(Column name _) -> ColumnName name Nothing) columns
    _ -> columnNames

selectColumns :: DataFrame -> [ColumnName] -> DataFrame
selectColumns (DataFrame dataColumns dataRows) parsedColumns =
  let checkedColumns = checkForStar parsedColumns dataColumns
      selectedColumnIndexes = myMapMaybe (\(ColumnName colName _) -> getColumnIndex colName dataColumns) checkedColumns
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
          BoolValue b -> map toLower (show b) == map toLower value
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

handleAggregate :: DataFrame -> [ColumnName] -> DataFrame
handleAggregate (DataFrame dataColumns dataRows) parsedColumns =
  let containsAggregate = any isAggregate parsedColumns

      isAggregate (ColumnName _ (Just Min)) = True
      isAggregate (ColumnName _ (Just Avg)) = True
      isAggregate _ = False

  in if not containsAggregate
    then DataFrame dataColumns dataRows
    else
      let
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

        getValueType (IntegerValue _) = IntegerType
        getValueType (StringValue _) = StringType
        getValueType (BoolValue _) = BoolType
        getValueType NullValue = StringType


      in case head parsedColumns of
            ColumnName name (Just Min) ->
              let
                columnIndex = (\(Just index) -> index) $ getColumnIndex name dataColumns
                values = map (!! columnIndex) dataRows
                aggregateValue = applyMin values
              in
                DataFrame [Column "minimum" (getValueType aggregateValue)] [[aggregateValue]]
            ColumnName name (Just Avg) ->
              let
                columnIndex = (\(Just index) -> index) $ getColumnIndex name dataColumns
                values = map (!! columnIndex) dataRows
                aggregateValue = applyAvg values
              in
                DataFrame [Column "average" (getValueType aggregateValue)] [[aggregateValue]]



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