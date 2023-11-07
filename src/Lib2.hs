{-# OPTIONS_GHC -Wno-partial-fields #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

{-# HLINT ignore "Use lambda" #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Lib2
  ( parseStatement,
    ParsedStatement,
  )
where

import Control.Applicative
import Data.Char (isAlphaNum, toLower)
import DataFrame (DataFrame)
import InMemoryTables (TableName)
import Data.Functor

type Database = [(TableName, DataFrame)]

type ErrorMessage = String

type ColumnName = String

type Opperation = String

data ValueExpr
  = Name ColumnName -- colName
  | AggMin ColumnName -- MIN(colName)
  | AggAvg ColumnName -- AVG(colName)
  | BinOp ValueExpr Opperation ValueExpr -- f(x) -> a '=' k  AND (...) AND (...) -> bool
  deriving (Eq, Show)

-- Keep the type, modify constructors
data ParsedStatement
  = Select
      { qeSelectList :: [ValueExpr], -- (min(colname), newColName)... select min(colname) as "newColName", [(ValueExpr, Maybe ColumnName)] jei gali but AS
        qeFrom :: [TableName], -- FROM a, b, c
        qeWhere :: Maybe ValueExpr
      }
  | ShowTables
  | ShowTable TableName
  deriving (Eq, Show)

newtype Parser a = Parser
  { runParser :: String -> Either ErrorMessage a
  }

-- Helper functions for parsing specific tokens

charP :: Char -> Parser Char
charP a = Parser $ \inp ->
  case inp of
    (x : xs) ->
      if a == x
        then Right a
        else Left ([a] ++ " expected but " ++ [x] ++ " found")

stringP :: String -> Parser String
stringP = traverse charP -- charP 'S', charP 'E', charP 'L' ... ar veikia???

keywordP :: String -> Parser String
keywordP keyword = Parser $ \inp ->
  if map toLower (take l inp) == map toLower keyword
    then Right keyword
    else Left $ keyword ++ " expected"
  where
    l = length keyword

spaceP :: Parser ()
spaceP = () <$ many (charP ' ')

spaceCommaP :: Parser () -- return left jei nera kablelio
spaceCommaP = do
  _ <- optional spaceP -- average
  _ <- charP ','
  _ <- optional spaceP
  return ()

-- selectListP :: Parser [ValueExpr]
-- selectListP = someFunction (valueExprP <* spaceCommaP) (keywordP "FROM")
manyTillCustom :: Parser a -> Parser end -> Parser [a]
manyTillCustom p end = scan
  where
    scan = (end $> []) <|> ((:) <$> p <*> scan)

selectListP :: Parser [ValueExpr]
selectListP = manyTillCustom (valueExprNoBinP <* spaceCommaP) (keywordP "FROM")

-- Parsing functions for the ValueExpr data type

parseName :: Parser String
parseName = Parser $ \inp ->
  case takeWhile isAlphaNum inp of
    [] -> Left "Empty input"
    xs -> Right xs

nameP :: Parser ValueExpr
nameP = Name <$> parseName

aggMinP :: Parser ValueExpr
aggMinP = AggMin <$> (stringP "MIN(" *> parseName <* charP ')')

aggAvgP :: Parser ValueExpr
aggAvgP = AggAvg <$> (stringP "AVG(" *> parseName <* charP ')')

binOpP :: Parser ValueExpr
binOpP =
  BinOp
    <$> (charP '(' *> valueExprP)
    <*> (spaceP *> many (charP '=') <* spaceP) -- nonsence
    <*> valueExprP
    <* charP ')'

valueExprP :: Parser ValueExpr
valueExprP = nameP <|> aggMinP <|> aggAvgP <|> binOpP

valueExprNoBinP :: Parser ValueExpr
valueExprNoBinP = nameP <|> aggMinP <|> aggAvgP

-- Parsing functions for the ParsedStatement data type

tableNameP :: Parser [TableName]
tableNameP = manyTillCustom (parseName <* spaceCommaP) (keywordP "WHERE") -- where nebutiani yra

tableNameParse :: Parser TableName
tableNameParse = (parseName <* spaceCommaP) <|> (parseName)

selectP :: Parser ParsedStatement
selectP =
  Select
    <$> (keywordP "SELECT" *> spaceP *> selectListP) -- gali but 0 spaces, FROM jau rastas, no new lines
    <*> (spaceP *> tableNameP)
    <*> optional (spaceP *> stringP "WHERE" *> spaceP *> valueExprP)

showTablesP :: Parser ParsedStatement
showTablesP = ShowTables <$ stringP "SHOW TABLES"

showTableP :: Parser ParsedStatement
showTableP = ShowTable <$> (stringP "SHOW TABLE" *> spaceP *> tableNameParse) -- only 1 table

parsedStatementP :: Parser ParsedStatement
parsedStatementP = selectP <|> showTablesP <|> showTableP

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement = runParser parsedStatementP
