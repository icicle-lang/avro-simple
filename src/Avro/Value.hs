{-| This module defines a representation of an Avro value.

One usually doesn't need to use this module,
as decoding straight to an Haskell domain type via a Codec
is better in most situations.

This type is exposed though as it can be used to build a
render function for Avro values of arbitrary types, which
could be useful for a monitoring tool.

-}
module Avro.Value (
    Value (..)
) where

import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
import Data.Map (Map)
import Data.Text (Text)


{-| Avro Value types

A Value in Avro can only be interpreted within the
context of a Schema.

For example, records are written in field order.

-}
data Value
    = Null
    | Boolean Bool
    | Int Int32
    | Long Int64
    | Float Float
    | Double Double
    | Bytes ByteString
    | String Text
    | Array [Value]
    | Map (Map Text Value)
    | Record [Value]
    | Union Int Value
    | Fixed ByteString
    | Enum Int
    deriving (Eq, Show)
