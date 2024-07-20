{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Avro.Internal.VarInt where

import           Avro.Internal.ReadSchema (ReadSchema)
import qualified Avro.Internal.ReadSchema as ReadSchema

import           Avro.Value (Value)
import qualified Avro.Value as Value
import           Avro.Name

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.Binary.Put (Put)
import qualified Data.Binary.Put as Put
import           Data.Map
import qualified Data.List as List
import           Data.Bits
import           Data.Binary (Word8, Word32, Word64)
import           Data.Foldable (traverse_)


getVarInt :: (Num i, Bits i) => Get i
getVarInt =
    let
        go depth acc = do
            b <- Get.getWord8
            let
                top =
                    b .&. 0x80

                dataBits =
                    b .&. 0x7F

                updated =
                    shiftL (fromIntegral dataBits) (7 * depth)
                        .|. acc

            if top == 0 then
                pure updated

            else
                go ( depth + 1 ) updated
    in
    go 0 0

{-# SPECIALIZE getVarInt :: Get Word32 #-}
{-# SPECIALIZE getVarInt :: Get Word64 #-}

putVarInt :: forall i. (Integral i, Bits i) => i -> Put
putVarInt =
    let
        go lower n =
            let
                top =
                    shiftR n 7

                finish =
                    top == 0

                base =
                    n .&. 0x7F

                encoded =
                    if finish then
                        fromIntegral base
                    else
                        fromIntegral base .|. 0x80

            in
            if finish then
                traverse_ Put.putWord8 (List.reverse (encoded : lower))

            else
                go (encoded : lower) top
    in
    go []

{-# SPECIALIZE putVarInt :: Word32 -> Put #-}
{-# SPECIALIZE putVarInt :: Word64 -> Put #-}
