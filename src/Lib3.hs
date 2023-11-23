{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    parseContentToDataFrame,
    getPath,
    writeDataFrameToYAML
  )
where

import Control.Monad.Free (Free (..), liftF)
import Data.Time ( UTCTime )
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row(..))
import Data.Yaml (decodeFileEither, FromJSON, parseJSON, withObject, (.:), (.:?), YamlException, ParseException)
import Control.Exception (try, IOException)
import qualified Data.Yaml as Y
import Data.Text (pack, unpack) 
import Data.Aeson.Key (fromString)
import qualified Data.ByteString.Char8 as BS
import Data.List (intercalate)
import Data.Char (toLower)
import InMemoryTables (database)


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
  | ParseStringOfFile FileContent ((Either ErrorMessage DataFrame) -> next)
  | SerializeDataFrameToYAML TableName DataFrame (DataFrame -> next)
  | CheckDataFrame DataFrame ((Either ErrorMessage DataFrame) -> next)
  -- feel free to add more constructors heref
  deriving Functor


parseContent :: FileContent -> Execution (Either ErrorMessage DataFrame)
parseContent fileContent = liftF $ ParseStringOfFile fileContent id

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

serializeDataFrameToYAML :: TableName -> DataFrame -> Execution DataFrame
serializeDataFrameToYAML tableName df = liftF $ SerializeDataFrameToYAML tableName df id

checkDataFrame :: DataFrame -> Execution (Either ErrorMessage DataFrame)
checkDataFrame df = liftF $ CheckDataFrame df id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql tableName = do
  fileContent <- loadFile tableName
  eitherDataFrame <- parseContent fileContent
  case eitherDataFrame of
    Left errMsg -> return (Left errMsg)
    Right df -> checkDataFrame df
    





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