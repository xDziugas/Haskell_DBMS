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
import DataFrame (DataFrame(..), Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
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
import Lib2 (parseStatement, ParsedStatement(..), Condition(..), ValueExpr(..))


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
  | ExecuteSelect [DataFrame] ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteInsert DataFrame ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteUpdate DataFrame ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteDelete DataFrame ParsedStatement (Either ErrorMessage DataFrame -> next)
  deriving Functor


loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

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
                      Right df -> chain ts (dfs ++ [df])
                      Left errMsg -> return $ Left errMsg


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
            Right df -> 
                executeUpdate df stmt >>= \updateResult ->
                case updateResult of
                    Right updatedDf -> do
                        -- Execute serialization and return its result
                        serializedResult <- serializeDataFrameToYAML tableName updatedDf
                        return $ Right serializedResult
                    Left errMsg -> 
                        return $ Left errMsg
            Left errMsg -> 
                return $ Left errMsg

    Right stmt@(Delete tableName _) ->
        loadFile tableName >>= \fileContent -> 
        parseFileContent fileContent >>= \eitherDf ->
        case eitherDf of
            Right df -> executeDelete df stmt
            Left errMsg -> return $ Left errMsg

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

