{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
  )
where

import Control.Monad.Free (Free (..), liftF)
import Data.Time ( UTCTime )
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row(..))
import Data.Yaml (decodeFileEither, FromJSON, parseJSON, withObject, (.:), (.:?), YamlException, ParseException)

import qualified Data.Yaml as Y
import Data.Aeson.Key (fromString)


type TableName = String
type FileContent = String
type ErrorMessage = String

data YamlTable = YamlTable
  { columns :: [YamlColumn]
  , rows :: [[YamlValue]]
  } deriving (Show, Eq)

data YamlColumn = YamlColumn
  { columnName :: String
  , columnType :: String
  } deriving (Show, Eq)

data YamlValue = YamlInt Int | YamlString String | YamlBool Bool | YamlNull
  deriving (Show, Eq)

type Execution = Free ExecutionAlgebra


data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)  
  | GetTime (UTCTime -> next)
  | ParseContentsIntoDataFrame FileContent (DataFrame -> next)
  -- feel free to add more constructors here
  deriving Functor



loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

parseContentsIntoDataFrame :: FileContent -> Execution DataFrame
parseContentsIntoDataFrame content = liftF $ ParseContentsIntoDataFrame content id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    return $ Left "implement me"


justCheck :: String -> IO String
justCheck fileName = do
  let relativePath = getPath fileName
  fileContents <- readFile relativePath
  return $ fileContents


getPath :: String -> String
getPath tableName = "db/" ++ tableName ++ ".yaml"