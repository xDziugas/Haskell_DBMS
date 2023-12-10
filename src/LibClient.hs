
module LibClient where

import Network.HTTP.Client
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Network.HTTP.Types.Method (methodPost)
import Network.HTTP.Types.Header (hContentType)
import Data.Aeson (encode, eitherDecode)
import Data.ByteString.Lazy (ByteString)
import Data.ByteString.Char8 (pack)
import DataFrame (DataFrame)

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