{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame(..), Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import Data.Time ( UTCTime )
import Lib2 (parseStatement, ParsedStatement(..), Condition(..), ValueExpr(..))
import InMemoryTables qualified
import Data.Char (toLower)
import Data.List (findIndices, findIndex)


type TableName = String
type FileContent = String
type ErrorMessage = String



data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)  
  | SaveFile TableName DataFrame next
  | ParseStringOfFile FileContent ((Either ErrorMessage DataFrame) -> next)
  | GetTime (UTCTime -> next)
  | ExecuteSelect [DataFrame] ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteInsert DataFrame ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteUpdate DataFrame ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteDelete DataFrame ParsedStatement (Either ErrorMessage DataFrame -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

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

-- Main function to construct SQL command execution steps
loadAndParseMultipleTables :: [TableName] -> Execution (Either ErrorMessage [DataFrame])
loadAndParseMultipleTables tables = chain tables []
  where
    chain :: [TableName] -> [DataFrame] -> Execution (Either ErrorMessage [DataFrame])
    chain [] dfs = return $ Right dfs
    chain (t:ts) dfs = loadFile t >>= \fileContent -> 
                    parseFileContent fileContent >>= \eitherDf ->
                    case eitherDf of
                      Right df -> chain ts (dfs ++ [df])
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
            Right df -> executeInsert df stmt
            Left errMsg -> return $ Left errMsg

    Right stmt@(Update tableName _ _) ->
        loadFile tableName >>= \fileContent -> 
        parseFileContent fileContent >>= \eitherDf ->
        case eitherDf of
            Right df -> executeUpdate df stmt
            Left errMsg -> return $ Left errMsg

    Right stmt@(Delete tableName _) ->
        loadFile tableName >>= \fileContent -> 
        parseFileContent fileContent >>= \eitherDf ->
        case eitherDf of
            Right df -> executeDelete df stmt
            Left errMsg -> return $ Left errMsg

    Left errorMsg -> return $ Left errorMsg

