
module LibClient (accpetResponseAndRender) where

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
import Lib2 (ParsedStatement(..), Condition(..), ValueExpr(..), parseStatement)
import qualified Lib1
import DataFrame (DataFrame(..), Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import GHC.Generics (Generic)

instance ToJSON ParsedStatement
instance ToJSON Condition
instance ToJSON ValueExpr

instance FromJSON DataFrame
instance FromJSON Column
instance FromJSON ColumnType
instance FromJSON Value

type ErrorMessage = String

decodeDataFrame :: BSL.ByteString -> Maybe DataFrame
decodeDataFrame = decode


encodeStatement :: ParsedStatement -> BSL.ByteString
encodeStatement = encode



sendHttpRequest :: String -> String -> IO (Either String DataFrame)
sendHttpRequest url sqlQuery = do
    manager <- newManager tlsManagerSettings
    initialRequest <- parseRequest url
    let request = initialRequest
            { method = methodPost
            , requestBody = RequestBodyLBS (encode sqlQuery)
            , requestHeaders = [(hContentType, pack "application/json")]
            }

    response <- httpLbs request manager
    -- return $ 'decode' $ responseBody response
    return $ Left "not implemented"


--TESTING 

accpetResponseAndRender :: BSL.ByteString -> Either ErrorMessage String
accpetResponseAndRender encodedDF = do
    case decodeDataFrame encodedDF of
        Nothing -> Left $ "something went wrong with decoding dataFrame"
        Just df -> Right $ Lib1.renderDataFrameAsTable 80 df



--sitas veikia bet komentuotas nes circular dependency right now

-- sendToServer :: String -> Either ErrorMessage String
-- sendToServer querry = do
--     case parseStatement querry of
--         Left errMsg -> Left $ "parsing not working"
--         Right parsedStatement -> do
--             let encodedStatement = encodeStatement parsedStatement
--             case Lib3.receiveStatement encodedStatement of
--                 Left errMsg -> Left $ errMsg
--                 Right succesMsg -> Right $ succesMsg