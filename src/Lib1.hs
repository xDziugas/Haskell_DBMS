{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import Data.Char (isAlpha, isAlphaNum, toLower)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName db search = lookup (map toLower search) [(map toLower name, dat) | (name, dat) <- db]

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement statement =
  case map (map toLower) (words statement) of
    ["select", "*", "from", tableName] ->
      if last tableName == ';'
        then
          if isValidName (init tableName)
            then Right (init tableName)
            else Left "Invalid select statement"
        else
          if isValidName tableName
            then Right tableName
            else Left "Invalid select statement"
    _ -> Left "Invalid select statement"
  where
    isValidName :: String -> Bool -- check if it has any symbols ((isSymbol x = False) && (isNumber x || x = '_'))
    isValidName name =
      not (null name) && isAlphaOrUnderscore (head name) && all isAlphaNumOrUnderscore (tail name)
      where
        isAlphaNumOrUnderscore :: Char -> Bool
        isAlphaNumOrUnderscore c = isAlphaNum c || c == '_'
        isAlphaOrUnderscore :: Char -> Bool
        isAlphaOrUnderscore c = isAlpha c || c == '_'

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame columns rows) =
  if all (\row -> length row == length columns) rows
    then
      if all (\(columnIndex, col) -> all (\row -> checkColumnType col (row !! columnIndex)) rows) (zip [0 ..] columns)
        then Right ()
        else Left "Data types are incorrect"
    else Left "Row lengths do not match the number of columns"

checkColumnType :: Column -> Value -> Bool
checkColumnType (Column _ columnType) value =
  case (columnType, value) of
    (IntegerType, IntegerValue _) -> True
    (StringType, StringValue _) -> True
    (BoolType, BoolValue _) -> True
    (_, NullValue) -> True
    _ -> False

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)

valueToString :: Value -> String
valueToString (IntegerValue x) = "| " ++ show x ++ " "
valueToString (StringValue x) = "| " ++ x ++ " "
valueToString (BoolValue x) = "| " ++ show x ++ " "
valueToString NullValue = "| NULL "

truncateName :: Integer -> String -> String
truncateName maxWidth name
  | length name > fromIntegral maxWidth =
      let excessWidth = length name - fromIntegral maxWidth + 3
          truncatedName = take (length name - excessWidth) name
       in truncatedName ++ ".. "
  | otherwise =
      let paddingWidth = fromIntegral maxWidth - length name
          paddedName = name ++ replicate paddingWidth ' '
       in paddedName

renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable width (DataFrame columns rows) =
  let numColumns = length columns

      columnWidth = width `div` fromIntegral numColumns

      columnNamesWithSeparator = map (\(Column name _) -> "| " ++ name ++ " ") columns

      truncatedColumnNames = map (\name -> truncateName columnWidth name) columnNamesWithSeparator

      dataRows = map renderRow rows

      tableContent =
        [horizontalLine]
          ++ [concat truncatedColumnNames]
          ++ [horizontalLine]
          ++ concatMap (\row -> [concat row, horizontalLine]) dataRows

      renderRow :: Row -> [String]
      renderRow row = map (truncateName columnWidth . valueToString) row

      horizontalLine :: String
      horizontalLine = replicate (fromIntegral width) '-'
   in unlines tableContent