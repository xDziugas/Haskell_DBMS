{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified LibServer
import Network.Wai
import Network.Wai.Handler.Warp
import Network.HTTP.Types (status200, status400)
import Data.Aeson (encode, eitherDecode)
import Data.ByteString.Lazy.Char8 (pack)
import Control.Monad.IO.Class (liftIO)
import LibServer (startServer, handleRequest, InMemoryData)

main :: IO ()
main = do
    inMemoryData <- LibServer.startServer ["employees", "sales", "products"]
    startHttpServer inMemoryData


startHttpServer :: InMemoryData -> IO ()
startHttpServer inMemoryData = run 4200 app
  where
    app req respond = do
      body <- strictRequestBody req
      -- case 'decode' body of
      case (Left "not implemented") of --TODO
        Right parsedStmt -> do
          result <- liftIO $ handleRequest inMemoryData parsedStmt
          case result of
            Right df -> respond $ responseLBS status200 [("Content-Type", "application/json")] ("encode df")
            Left err -> respond $ responseLBS status400 [("Content-Type", "text/plain")] (pack err)
        Left err -> respond $ responseLBS status400 [("Content-Type", "text/plain")] (pack $ "Invalid request: " ++ err)
