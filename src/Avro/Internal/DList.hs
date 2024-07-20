module Avro.Internal.DList where

type DList a = [a] -> [a]


append :: DList a -> DList a -> DList a
append =
    (.)


singleton :: a -> DList a
singleton x xs =
    x : xs


empty :: DList a
empty =
    id


toList :: DList a -> [a]
toList d =
    d []
