{-# LANGUAGE DeriveGeneric #-}

module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

import GHC.Generics (Generic)

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  deriving (Generic, Show, Eq)

data Column = Column String ColumnType
  deriving (Generic, Show, Eq)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  deriving (Generic, Show, Eq)

type Row = [Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Generic, Show, Eq)
