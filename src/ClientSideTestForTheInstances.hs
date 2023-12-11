{-# LANGUAGE DeriveGeneric #-}

module ClientSideTestForTheInstances(
    renderdf
) where


import Data.Aeson(decode, encode, parseJSON, ToJSON, FromJSON)
import qualified Data.ByteString.Lazy as BSL
import Lib2 (ParsedStatement(..), Condition(..), ValueExpr(..), parseStatement)
import qualified Lib1
-- sito reikia jei norim patestint siuntima is serverio i client'a nes kitaip circular dependecy bus todel komentuotas 
-- import qualified Lib3
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
decodeDataFrame encodedDF = decode encodedDF


encodeStatement :: ParsedStatement -> BSL.ByteString
encodeStatement statement = encode statement


renderdf :: BSL.ByteString -> Either ErrorMessage String
renderdf encodedDF = do
    case decodeDataFrame encodedDF of
        Nothing -> Left $ "something went wrong with decoding dataFrame"
        Just df -> Right $ Lib1.renderDataFrameAsTable 80 df



--sitas veikia bet komentuotas nes circular dependency

-- sendToServer :: String -> Either ErrorMessage String
-- sendToServer querry = do
--     case parseStatement querry of
--         Left errMsg -> Left $ "parsing not working"
--         Right parsedStatement -> do
--             let encodedStatement = encodeStatement parsedStatement
--             case Lib3.receiveStatement encodedStatement of
--                 Left errMsg -> Left $ errMsg
--                 Right succesMsg -> Right $ succesMsg





