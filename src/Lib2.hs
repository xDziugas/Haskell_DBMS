{-# OPTIONS_GHC -Wno-partial-fields #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

{-# HLINT ignore "Use lambda" #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# LANGUAGE InstanceSigs #-}
{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# HLINT ignore "Use newtype instead of data" #-}
{-# HLINT ignore "Use lambda-case" #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Lib2
  ( parseStatement,
    ParsedStatement (..),
    executeStatement,
    Condition(..),
    ValueExpr(..)
  )
where

import Control.Applicative ( Alternative(..) )
import Data.Char (isAlphaNum)
import DataFrame (DataFrame)
import InMemoryTables (TableName)
import Control.Monad (void)
import Prelude hiding (elem)

import Control.Monad.Trans.State.Strict (State, get, put, evalState, runState)
import GHC.Unicode (toLower)

type Database = [(TableName, DataFrame)]

type ErrorMessage = String

newtype EitherT e m a = EitherT {
  runEitherT :: m (Either e a)
}

newtype Parser a = Parser {
  runParser :: EitherT ErrorMessage (State String) a
} deriving (Functor, Applicative, Monad)

instance Monad m => Functor (EitherT e m) where
  fmap f (EitherT ema) = EitherT $ (fmap . fmap) f ema


instance Monad m => Applicative (EitherT e m) where
  pure :: Monad m => a -> EitherT e m a
  pure = EitherT . pure . pure
  (<*>) :: Monad m => EitherT e m (a -> b) -> EitherT e m a -> EitherT e m b
  (EitherT fab) <*> (EitherT ema) = EitherT $ (<*>) <$> fab <*> ema

instance Alternative Parser where
  empty = throwError "Empty alternative"
  Parser p1 <|> Parser p2 = Parser $ EitherT $ do
    state <- get
    case runState (runEitherT p1) state of
      (Right x, newState) -> put newState >> return (Right x)
      (Left _, _) -> runEitherT p2


instance Monad m => Monad (EitherT e m) where
  return :: Monad m => a -> EitherT e m a
  return = pure
  (>>=) :: Monad m => EitherT e m a -> (a -> EitherT e m b) -> EitherT e m b
  (EitherT ema) >>= f = EitherT $ do
    v <- ema
    case v of
      Left e -> return $ Left e
      Right a -> runEitherT (f a)

lift :: Monad m => m a -> EitherT e m a
lift = EitherT . fmap Right

throwError :: ErrorMessage -> Parser a
throwError msg = Parser $ EitherT $ return $ Left msg

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented"

-------------- data, types----------------------

data Order
  = Asc String
  | Desc String
  deriving (Eq, Show)

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
  | Now
  deriving (Eq, Show)

data ParsedStatement
  = Select
      { qeSelectList :: [ValueExpr],
        qeFrom :: [TableName],
        qeWhere :: Maybe [Condition],
        qeOrderBy :: Maybe [Order]
      }
  | Insert TableName [String] [String] -- table, columns, values
  | Delete TableName (Maybe [Condition]) -- TableName, Optional Conditions
  | Update TableName [Condition] (Maybe [Condition]) -- TableName, Set Conditions, Optional Where Conditions
  | ShowTables
  | ShowTable TableName
  | Create TableName [String] [String] -- name, columns, types
  | Drop TableName
  deriving (Eq, Show)

-----------------parsers----------------------

many' :: Show a => Parser a -> Parser [a]
many' p = manyAccum p []

manyAccum :: Parser a -> [a] -> Parser [a]
manyAccum p acc = (do
    x <- p
    manyAccum p (acc ++ [x])
  ) <|> return acc

some' :: Show a => Parser a -> Parser [a]
some' p = do
    x <- p
    xs <- many' p
    return (x:xs)

optional' :: Parser a -> Parser (Maybe a)
optional' p = (Just <$> p) <|> pure Nothing


-- Parses a char
charP :: Char -> Parser Char
charP a = satisfy (== a)

charIgnoreCaseP :: Char -> Parser Char
charIgnoreCaseP c = satisfy (\x -> toLower x == toLower c)

satisfy :: (Char -> Bool) -> Parser Char
satisfy predicate = Parser $ EitherT $ do
  inpBefore <- get
  case inpBefore of
    [] -> return $ Left "Unexpected end of input"
    (x:xs) ->
      if predicate x
      then do
        put xs
        return (Right x)
      else return $ Left ("Character " ++ show x ++ " does not satisfy the predicate")

stringP :: String -> Parser String
stringP = traverse charP

stringIgnoreCaseP :: String -> Parser String
stringIgnoreCaseP = traverse charIgnoreCaseP

-- Parses >=0 spaces
spaceP :: Parser ()
spaceP = void $ many' $ charP ' '

-- Parses >=0 spaces, 1 comma and >=0 spaces
commaSpaceP :: Parser ()
commaSpaceP = do
  optional' spaceP
  charP ','
  optional' spaceP
  return ()

tokenP :: Parser a -> Parser a
tokenP p = p <* spaceP -- space???

-- Parses a given keyword, case insensitive
keywordP :: String -> Parser String
keywordP w = tokenP $ stringIgnoreCaseP w

-- Parse identifier (alphanumeric name)
identifierP :: Parser String
identifierP = tokenP $ some' $ satisfy (\c -> isAlphaNum c || c == '_')

-- Parses string literal (i.e. "multiple words string"), neveikia, reik uztestuot
stringLiteralP :: Parser String
stringLiteralP = charP '"' *> many' (satisfy (/= '"')) <* charP '"'

-- Parse condition (colName operation value/string)
conditionP :: Parser Condition
conditionP = do
  col <- identifierP
  optional' spaceP
  op <- conditionOperatorP
  optional' spaceP
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
aggMinP = AggMin <$> (stringP "MIN" *> optional' spaceP *> charP '(' *> optional' spaceP *> identifierP <* optional' spaceP <* charP ')' <* optional' spaceP)

-- Parses aggregate function AVG(colName)
aggAvgP :: Parser ValueExpr
aggAvgP = AggAvg <$> (stringP "AVG" *> optional' spaceP *> charP '(' *> optional' spaceP *> identifierP <* optional' spaceP <* charP ')' <* optional' spaceP)

-- Parses any value expression (data type ValueExpr contains SELECTED columns)
valueExprP :: Parser ValueExpr
valueExprP = aggMinP <|> aggAvgP <|> nowP <|> nameP

-- Parses a list of values while condition is met (i.e. until a comma is found) 
sepBy :: Show a => Parser a -> Parser sep -> Parser [a]
sepBy p sep = sepBy1 p sep <|> pure []

sepBy1 :: Show a => Parser a -> Parser sep -> Parser [a]
sepBy1 p sep = do
  x <- p
  xs <- many' (sep >> p)
  return (x:xs)


----------------------3rd task parsers-----------------------

nowP :: Parser ValueExpr
nowP = Now <$ keywordP "NOW()"

-- Parser for INSERT statement
insertP :: Parser ParsedStatement
insertP = do
  void $ keywordP "INSERT INTO"
  spaceP
  tableName <- identifierP
  optional' spaceP
  columns <- between (charP '(') (charP ')') (identifierP `sepBy` commaSpaceP) -- nebutina between naudot mb??
  optional' spaceP
  void $ keywordP "VALUES"
  optional' spaceP
  values <- between (charP '(') (charP ')') (identifierP `sepBy` commaSpaceP) -- string parserio reik mb??
  return $ Insert tableName columns values

-- Parser for DELETE statement
deleteP :: Parser ParsedStatement
deleteP = do
  void $ keywordP "DELETE FROM"
  spaceP
  tableName <- identifierP
  whereExpr <- optional' (spaceP *> keywordP "WHERE" *> spaceP *> conditionsP)
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
  whereExpr <- optional' (spaceP *> keywordP "WHERE" *> spaceP *> conditionsP)
  return $ Update tableName setConditions whereExpr

-- Utility function for parsing between two characters
between :: Parser open -> Parser close -> Parser a -> Parser a
between open close p = do
  void open
  x <- p
  void close
  return x

--------------------------4th task pasrsers------------------------------------------

orderP :: Parser Order
orderP = do
  colName <- identifierP
  orderType <- (keywordP "ASC" *> pure Asc) <|> (keywordP "DESC" *> pure Desc)
  return $ orderType colName


orderByP :: Parser [Order]
orderByP = keywordP "ORDER BY" *> (orderP `sepBy` commaSpaceP)

createP :: Parser ParsedStatement
createP = do
  void $ keywordP "CREATE TABLE"
  tableName <- identifierP
  columns <- between (charP '(') (charP ')') (columnDefP `sepBy` commaSpaceP)
  return $ Create tableName (map fst columns) (map snd columns)

columnDefP :: Parser (String, String)
columnDefP = do
  colName <- identifierP
  colType <- identifierP
  return (colName, colType)

dropP :: Parser ParsedStatement
dropP = Drop <$> (keywordP "DROP TABLE" *> identifierP)


-------------------------------------------------------------------------------------
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
  whereExpr <- optional' (spaceP *> keywordP "WHERE" *> spaceP *> conditionsP)
  orderBy <- optional' orderByP
  return $ Select cols tables whereExpr orderBy

showTablesP :: Parser ParsedStatement
showTablesP = ShowTables <$ keywordP "SHOW TABLES"

showTableP :: Parser ParsedStatement
showTableP = ShowTable <$> (keywordP "SHOW TABLE" *> identifierP)

-- Try to parse any statements
parsedStatementP :: Parser ParsedStatement
parsedStatementP = selectP <|> showTablesP <|> showTableP <|> insertP <|> deleteP <|> updateP <|> createP <|> dropP

-- Parse statement, start
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input = evalState (runEitherT $ runParser (parsedStatementP <* charP ';')) input