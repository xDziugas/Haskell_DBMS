{-# OPTIONS_GHC -Wno-partial-fields #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

{-# HLINT ignore "Use lambda" #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# LANGUAGE InstanceSigs #-}
{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# HLINT ignore "Use newtype instead of data" #-}
{-# HLINT ignore "Use lambda-case" #-}

module Lib2
  ( 
    parseStatement,
    ParsedStatement(..), ValueExpr(..), Condition(..),
    ValueExpr,
    Condition,
  )
where

import Control.Applicative ( Alternative(..), optional )
import Data.Char (isAlphaNum, toLower)
import DataFrame (DataFrame, Column)
import InMemoryTables (TableName)
import Control.Monad (void)

type Database = [(TableName, DataFrame)]

type ErrorMessage = String

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (String, a)
}

instance Functor Parser where
  fmap :: (a -> b) -> Parser a -> Parser b
  fmap f functor = Parser $ \inp ->
    case runParser functor inp of
        Left e -> Left e
        Right (l, a) -> Right (l, f a)

instance Applicative Parser where
  pure :: a -> Parser a
  pure a = Parser $ \inp -> Right (inp, a)
  (<*>) :: Parser (a -> b) -> Parser a -> Parser b
  ff <*> fa = Parser $ \in1 ->
    case runParser ff in1 of
        Left e1 -> Left e1
        Right (in2, f) -> case runParser fa in2 of
            Left e2 -> Left e2
            Right (in3, a) -> Right (in3, f a)

instance Alternative Parser where
  empty :: Parser a
  empty = Parser $ \_ -> Left "Error"
  (<|>) :: Parser a -> Parser a -> Parser a
  p1 <|> p2 = Parser $ \inp ->
    case runParser p1 inp of
        Right r1 -> Right r1
        Left _ -> case runParser p2 inp of
            Right r2 -> Right r2
            Left e -> Left e

instance Monad Parser where
  (>>=) :: Parser a -> (a -> Parser b) -> Parser b
  ma >>= mf = Parser $ \inp1 ->
    case runParser ma inp1 of
        Left e1 -> Left e1
        Right (inp2, a) -> case runParser (mf a ) inp2 of
            Left e2 -> Left e2
            Right (inp3, r) -> Right (inp3, r)

-------------- data, types----------------------

data ColumnName = ColumnName String
  deriving (Eq, Show)

data Condition
  = Equals String String
  | LessThan String String
  | GreaterThan String String
  | LessEqualThan String String
  | GreaterEqualThan String String
  deriving (Show, Eq)

data ValueExpr
  = Name String
  | AggMin String
  | AggAvg String
  deriving (Eq, Show)

data ParsedStatement
  = Select
      { qeSelectList :: [ValueExpr],
        qeFrom :: [TableName],
        qeWhere :: Maybe [Condition]
      }
  | Insert TableName [String] [String] -- table, columns, values
  | Delete TableName (Maybe [Condition]) -- TableName, Optional Conditions
  | Update TableName [Condition] (Maybe [Condition]) -- TableName, Set Conditions, Optional Where Conditions
  | ShowTables
  | ShowTable TableName
  | Now
  deriving (Eq, Show)

-----------------parsers----------------------

-- Parses a char
charP :: Char -> Parser Char
charP x = Parser f
  where
    f (y:ys)
      | toLower y == toLower x = Right (ys, x)
      | otherwise = Left "Unexpected char"
    f [] = Left "Unexpected input"

stringP :: String -> Parser String
stringP = traverse charP

-- Parses >=0 spaces
spaceP :: Parser ()
spaceP = void $ many $ charP ' '

-- Parses >=0 spaces, 1 comma and >=0 spaces
commaSpaceP :: Parser ()
commaSpaceP = do
  optional spaceP
  charP ','
  optional spaceP
  return ()

tokenP :: Parser a -> Parser a
tokenP p = p <* spaceP -- space???

-- Parses a given keyword
keywordP :: String -> Parser String
keywordP w = tokenP $ stringP w

-- Parse identifier (alphanumeric name)
identifierP :: Parser String
identifierP = tokenP $ some $ satisfy (\c -> isAlphaNum c || c == '_' || c == '*')

-- Parses string literal (i.e. "multiple words string"), neveikia, reik uztestuot
stringLiteralP :: Parser String
stringLiteralP = charP '"' *> many (satisfy (/= '"')) <* charP '"'

-- Parse condition (colName operation value/string)
conditionP :: Parser Condition
conditionP = do
  col <- identifierP
  optional spaceP
  op <- conditionOperatorP
  optional spaceP
  value <- identifierP <|> stringLiteralP -- Assuming stringLiteralP parses "string" su kabutem
  return $ op col value -- del nothing neveikia col=col1=col2???

-- Parse a condition operator to Condition data type
conditionOperatorP :: Parser (String -> String -> Condition)
conditionOperatorP = (Equals           <$ stringP "=")
                 <|> (LessEqualThan    <$ stringP "<=")
                 <|> (GreaterEqualThan <$ stringP ">=")
                 <|> (LessThan         <$ stringP "<")
                 <|> (GreaterThan      <$ stringP ">")

-- Parse conditions until there is no keyword "AND"
conditionsP :: Parser [Condition]
conditionsP = conditionP `sepBy` keywordP "AND"

updateConditionsP :: Parser [Condition]
updateConditionsP = conditionP `sepBy` commaSpaceP

nameP :: Parser ValueExpr
nameP = Name <$> identifierP

-- Parses aggregate function MIN(colName)
aggMinP :: Parser ValueExpr
aggMinP = AggMin <$> (stringP "MIN" *> optional spaceP *> charP '(' *> optional spaceP *> identifierP <* optional spaceP <* charP ')' <* optional spaceP)

-- Parses aggregate function AVG(colName)
aggAvgP :: Parser ValueExpr
aggAvgP = AggAvg <$> (stringP "AVG" *> optional spaceP *> charP '(' *> optional spaceP *> identifierP <* optional spaceP <* charP ')' <* optional spaceP)

-- Parses any value expression (data type ValueExpr contains SELECTED columns)
valueExprP :: Parser ValueExpr
valueExprP = aggMinP <|> aggAvgP <|> nameP

-- Parses a list of values while condition is met (i.e. until a comma is found) 
sepBy :: Parser a -> Parser sep -> Parser [a]
sepBy p sep = sepBy1 p sep <|> pure []

sepBy1 :: Parser a -> Parser sep -> Parser [a]
sepBy1 p sep = do
  x <- p
  xs <- many (sep >> p)
  return (x:xs)

satisfy :: (Char -> Bool) -> Parser Char
satisfy predicate = Parser $ \input ->
  case input of
    (x:xs) | predicate x -> Right (xs, x)
    _ -> Left "Unexpected input"

----------------------3rd task parsers-----------------------

nowP :: Parser ParsedStatement
nowP = Now <$ keywordP "NOW()"

-- Parser for INSERT statement
insertP :: Parser ParsedStatement
insertP = do
  void $ keywordP "INSERT INTO"
  spaceP
  tableName <- identifierP
  optional spaceP
  columns <- between (charP '(') (charP ')') (identifierP `sepBy` commaSpaceP) -- nebutina between naudot mb??
  optional spaceP
  void $ keywordP "VALUES"
  optional spaceP
  values <- between (charP '(') (charP ')') (identifierP `sepBy` commaSpaceP) -- string parserio reik mb??
  return $ Insert tableName columns values

-- Parser for DELETE statement
deleteP :: Parser ParsedStatement
deleteP = do
  void $ keywordP "DELETE FROM"
  spaceP
  tableName <- identifierP
  whereExpr <- optional (spaceP *> keywordP "WHERE" *> spaceP *> conditionsP)
  return $ Delete tableName whereExpr

-- Parser for UPDATE statement
updateP :: Parser ParsedStatement
updateP = do
  void $ keywordP "UPDATE"
  spaceP
  tableName <- identifierP
  spaceP
  void $ keywordP "SET"
  spaceP
  setConditions <- updateConditionsP
  whereExpr <- optional (spaceP *> keywordP "WHERE" *> spaceP *> conditionsP)
  return $ Update tableName setConditions whereExpr

-- Utility function for parsing between two characters
between :: Parser open -> Parser close -> Parser a -> Parser a
between open close p = do
  void open
  x <- p
  void close
  return x

--------------------------------------------------------------------------

-- Parses a list of colNames until there is no comma (optimizable)
selectListP :: Parser [ValueExpr]
selectListP = valueExprP `sepBy` commaSpaceP

-- Parses a list of tableNames
tableNameListP :: Parser [String]
tableNameListP = identifierP `sepBy` commaSpaceP

-- SELECT statement parser
selectP :: Parser ParsedStatement
selectP = do
  void $ keywordP "SELECT"
  cols <- selectListP
  void $ keywordP "FROM"
  tables <- tableNameListP
  whereExpr <- optional (spaceP *> keywordP "WHERE" *> spaceP *> conditionsP)
  return $ Select cols tables whereExpr

showTablesP :: Parser ParsedStatement
showTablesP = ShowTables <$ keywordP "SHOW TABLES"

showTableP :: Parser ParsedStatement
showTableP = ShowTable <$> (keywordP "SHOW TABLE" *> identifierP)

-- Try to parse any statements
parsedStatementP :: Parser ParsedStatement
parsedStatementP = nowP <|> selectP <|> showTablesP <|> showTableP <|> insertP <|> deleteP <|> updateP

-- Parse statement, start
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input = case runParser (parsedStatementP <* charP ';') input of
  Right (_, result) -> Right result
  Left _ -> Left "Invalid statement"