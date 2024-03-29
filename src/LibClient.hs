
module LibClient (sendHttpRequest) where

import Network.HTTP.Client
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Network.HTTP.Types.Method (methodPost)
import Network.HTTP.Types.Header (hContentType)
import Data.Aeson (encode, eitherDecode)
import Data.ByteString.Lazy (ByteString)
import Data.ByteString.Char8 (pack)
import DataFrame (DataFrame)

import Data.Aeson(decode, encode, parseJSON, ToJSON, FromJSON)
import qualified Data.ByteString.Lazy as BSL
import Lib2 (ParsedStatement(..), Condition(..), ValueExpr(..), Order(..), parseStatement)
import qualified Lib1
import DataFrame (DataFrame(..), Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import GHC.Generics (Generic)

instance ToJSON ParsedStatement
instance ToJSON Condition
instance ToJSON ValueExpr
instance ToJSON Order

instance FromJSON DataFrame
instance FromJSON Column
instance FromJSON ColumnType
instance FromJSON Value

type ErrorMessage = String

decodeDataFrame :: BSL.ByteString -> Maybe DataFrame
decodeDataFrame = decode


encodeStatement :: ParsedStatement -> BSL.ByteString
encodeStatement = encode



sendHttpRequest :: String -> String -> IO (Either ErrorMessage DataFrame)
sendHttpRequest url sqlQuery = do
    manager <- newManager tlsManagerSettings
    initialRequest <- parseRequest url

    case parseAndEncode sqlQuery of
        Left errMsg -> return $ Left errMsg
        Right encodedQuery -> do
            let request = initialRequest
                    { method = methodPost
                    , requestBody = RequestBodyLBS encodedQuery
                    , requestHeaders = [(hContentType, pack "application/json")]
                    }

            response <- httpLbs request manager
            return $ case decodeDataFrame (responseBody response) of
                Just df -> Right df
                Nothing -> Left "Failed to decode response"



parseAndEncode :: String -> Either ErrorMessage BSL.ByteString
parseAndEncode query = do
    case parseStatement query of
        Left errMsg -> Left errMsg
        Right parsedStatement -> Right $ encodeStatement parsedStatement


