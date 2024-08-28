{-# LANGUAGE RecordWildCards #-}
{-| This module contains top level functions for converting Avro data to and from typed Haskell values.

It is designed to be used with the Codec module.

One should construct a Codec, then, use the functions below to read and write Avro encoded binary data.

-}
module Avro
    (
    -- * Parsing and writing Avro data
    makeDecoder, makeEncoder,

    -- * Working with named types
    -- $environments
    Environment (..), makeDecoderInEnvironment
    ) where

import qualified Avro.Codec as Codec
import qualified Avro.Internal.Bytes as Bytes
import           Avro.Internal.Deconflict (deconflict)
import           Avro.Schema (Schema, SchemaMismatch)
import           Data.Binary (Get, Put)
import qualified Data.Map as Map
import qualified Avro.Internal.Overlay as Overlay

{-| Create a binary decoder for avro data given a Codec and the writer's Schema.

Fields in Avro data types are not tagged, and records can only be interpreted
knowing the exact Schema with which they were written.

Therefore, building a binary decoder not requires a Codec for the type of
interest, but also the writer of the data's [`Schema`](Avro-Schema#Schema).

-}
makeDecoder :: Codec.Codec a -> Schema -> Either SchemaMismatch (Get a)
makeDecoder =
    makeDecoderInEnvironment (Environment [] [])

{-| Make a binary encoder for data using an Avro Codec
-}
makeEncoder :: Codec.Codec a -> a -> Put
makeEncoder codec =
    Bytes.encodeValue . Codec.writer codec


-- $environments
-- It is common when building Avro schemas traditionally to write small
-- schemas for parts of a larger record or set of messages. To do this,
-- one uses named types to create references so that schema definitions
-- can be simplified and reused.
--
-- In this library, this can be quite simply done for Codecs by using the
-- [`namedType`](Avro-Codec#namedType) function from the [`Codec`](Avro-Codec)
-- module.
--
-- To read and write data which is separated in this manner, one should first
-- construct an `Environment` for all named types used by the reader and
-- writer. Then, use that environment when constructing a decoder.

{-| Schemas which can be referred to by name in either the readers and writers.
-}
data Environment =
    Environment {
        readerEnvironment :: [Schema],
        writerEnvironment :: [Schema]
    }

{-| Create a binary decoder for avro data given a Codec and the writer's Schema.

This function will generate a decoder after considering the Named types in the
defined set of schemas.

-}
makeDecoderInEnvironment :: Environment -> Codec.Codec a -> Schema -> Either SchemaMismatch (Get a)
makeDecoderInEnvironment env Codec.Codec {..} writerSchema = do
    let
        overlayedCodec =
            Codec.Codec (Overlay.overlays schema (readerEnvironment env))
                decoder writer

        overlayedWriter =
            Overlay.overlays writerSchema (writerEnvironment env)

    readSchema <-
        deconflict Map.empty (Codec.schema overlayedCodec) overlayedWriter

    pure $ do
        values <-
            Bytes.makeDecoder Bytes.emptyEnvironment readSchema

        case Codec.decoder overlayedCodec values of
            Nothing ->
                fail "Couldn't extract typed value from parsed avro"
            Just a ->
                pure a


