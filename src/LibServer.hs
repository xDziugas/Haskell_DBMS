{-# LANGUAGE DeriveGeneric #-}

module LibServer
  (
    handleRequest,
    startServer,
    InMemoryData,
    decodeSentStatement,
    encodeDataFrame,
    getTableNames
  )
where


import DataFrame (DataFrame(..), Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import Data.Time ( UTCTime )
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row(..))
import Lib1 qualified
import Lib3 qualified
import Lib2 (parseStatement, ParsedStatement(..), Condition(..), ValueExpr(..))
import Data.List
--FOR TESTING
import LibClient qualified


import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Maybe (mapMaybe)
import Data.Time.Clock (getCurrentTime)
import System.Directory (listDirectory)
import System.FilePath (takeExtension, dropExtension)
import System.IO (writeFile)

import Data.Aeson(decode, encode, parseJSON, ToJSON, FromJSON)
import GHC.Generics (Generic)
import qualified Data.ByteString.Lazy as BSL

type TableName = String
type FileContent = String
type ErrorMessage = String
type InMemoryData = MVar [DataFrameWithTableName]

data DataFrameWithTableName = DataFrameWithTableName TableName DataFrame

instance FromJSON ParsedStatement
instance FromJSON Condition
instance FromJSON ValueExpr


instance ToJSON DataFrame
instance ToJSON Column
instance ToJSON ColumnType
instance ToJSON Value

decodeSentStatement :: BSL.ByteString -> Maybe ParsedStatement
decodeSentStatement = decode

encodeDataFrame :: DataFrame -> BSL.ByteString
encodeDataFrame = encode

-- Server startup
startServer :: [TableName] -> IO InMemoryData
startServer tableNames = do
    inMemoryData <- newMVar []
    mapM_ (loadTableData inMemoryData) tableNames
    _ <- forkIO $ periodicSave inMemoryData
    return inMemoryData

loadTableData :: InMemoryData -> TableName -> IO ()
loadTableData inMemoryData tableName = do
    eitherDf <- loadFile tableName
    case eitherDf of
        Right df -> modifyMVar_ inMemoryData (return . (++ [DataFrameWithTableName tableName df]))
        Left errMsg -> putStrLn $ "Error loading table " ++ tableName ++ ": " ++ errMsg

periodicSave :: InMemoryData -> IO ()
periodicSave inMemoryData = forever $ do
    threadDelay 1000000 -- Delay for 1 second
    dataFrames <- readMVar inMemoryData
    mapM_ saveTableData dataFrames

saveTableData :: DataFrameWithTableName -> IO ()
saveTableData (DataFrameWithTableName tableName df) = do
    eitherResult <- saveFile tableName df
    case eitherResult of
        Right _ -> return ()
        Left errMsg -> putStrLn $ "Error saving table " ++ tableName ++ ": " ++ errMsg


loadFile :: TableName -> IO (Either ErrorMessage DataFrame)
loadFile tableName = do
    let relativePath = Lib3.getPath tableName
    fileContentOrError <- try (readFile relativePath) :: IO (Either IOException FileContent)
    case fileContentOrError of
        Right fileContent -> return $ Lib3.parseContentToDataFrame fileContent
        Left _ -> return $ Left "File not found or unreadable"

saveFile :: TableName -> DataFrame -> IO (Either ErrorMessage DataFrame)
saveFile tableName df = do
    result <- try (Lib3.writeDataFrameToYAML tableName df) :: IO (Either IOException DataFrame)
    case result of
        Right writenDf -> return $ Right df
        Left _ -> return $ Left "Failed to write to file"

handleRequest :: InMemoryData -> ParsedStatement -> IO (Either ErrorMessage DataFrame)
handleRequest inMemoryData stmt = do
    dataFrames <- takeMVar inMemoryData
    case stmt of
        Select{qeFrom = tables} -> do
            let dfs = mapMaybe (\(DataFrameWithTableName tn df) -> if tn `elem` tables then Just df else Nothing) dataFrames
            currentTime <- getCurrentTime
            putMVar inMemoryData dataFrames
            return $ Lib3.executeSelectOperation dfs stmt currentTime
        Insert tableName _ _ -> processUpdateStatement tableName dataFrames stmt Lib3.executeInsertOperation
        Update tableName _ _ -> processUpdateStatement tableName dataFrames stmt Lib3.executeUpdateOperation
        Delete tableName _   -> processUpdateStatement tableName dataFrames stmt Lib3.executeDeleteOperation
        _ -> do
            putMVar inMemoryData dataFrames
            return $ Left "Unhandled statement type"
    where
        processUpdateStatement tableName dfs stmt operation = do
            let maybeDf = find (\(DataFrameWithTableName tn _) -> tn == tableName) dfs
            case maybeDf of
                Just (DataFrameWithTableName _ df) ->
                    case operation df stmt of
                        Right updatedDf -> do
                            let updatedDataFrames = DataFrameWithTableName tableName updatedDf : filter (\(DataFrameWithTableName tn _) -> tn /= tableName) dfs
                            putMVar inMemoryData updatedDataFrames
                            return $ Right updatedDf
                        Left errMsg -> do
                            putMVar inMemoryData dfs
                            return $ Left errMsg
                Nothing -> do
                    putMVar inMemoryData dfs
                    return $ Left $ "Table not found: " ++ tableName

getTableNames :: [String]
getTableNames = do
    let dbDir = "db"
    fileNames <- listDirectory dbDir
    let tableNames = map dropExtension $ filter (\f -> takeExtension f == ".yaml") fileNames
    return tableNames

-- saveTableNames :: IO [String]
-- saveTableNames = do
--     let dbDir = "db"
--     fileNames <- listDirectory dbDir
--     let tableNames = map dropExtension $ filter (\f -> takeExtension f == ".yaml") fileNames
--     let savePath = "db/tableNames.txt"
--     writeFile savePath (unlines tableNames)
--     return tableNames



---TESTING

-- receiveStatement :: BSL.ByteString -> Either ErrorMessage String
-- receiveStatement encodedStatement = do
--     case decodeSentStatement encodedStatement of 
--         Nothing -> Left "wasnt able to decode statement"
--         Just _ -> Right "the sent statement was received!!!!"



-- --cia encodinimas dataFram'o ir siuntimas i cliento puse
-- sendDataFrame :: String -> IO ()
-- sendDataFrame query = do
--     result <- Lib3.runExecuteIOTest $ Lib3.executeSql query
--     case result of
--         Left errMsg -> putStrLn $ "Error: " ++ errMsg
--         Right df -> do
--             let encodedDf = encodeDataFrame df
--             case LibClient.accpetResponseAndRender encodedDf of
--                 Left errMsg -> putStrLn $ "Error: " ++ errMsg
--                 Right success -> putStrLn success






