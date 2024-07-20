module Avro.Internal.ZigZag where

import Data.Int (Int32, Int64)
import Data.Bits
import Data.Word (Word32, Word64)

zig32 :: Int32 -> Word32
zig32 n =
    fromIntegral $
        (n `shiftL` 1) `xor` (n `shiftR` 31)


zig64 :: Int64 -> Word64
zig64 n =
    fromIntegral $
        (n `shiftL` 1) `xor` (n `shiftR` 63)


zag32 :: Word32 -> Int32
zag32 n =
    fromIntegral $
        (n `shiftR` 1) `xor` negate (n .&. 0x1)


zag64 :: Word64 -> Int64
zag64 n =
    fromIntegral $
        (n `shiftR` 1) `xor` negate (n .&. 0x1)
