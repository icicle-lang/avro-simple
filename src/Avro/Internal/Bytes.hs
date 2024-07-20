{-# LANGUAGE DoAndIfThenElse #-}
module Avro.Internal.Bytes where

import           Avro.Internal.ReadSchema (ReadSchema, ReadField)
import qualified Avro.Internal.ReadSchema as ReadSchema
import           Avro.Internal.VarInt (getVarInt, putVarInt)
import           Avro.Internal.ZigZag ( zag32, zag64, zig32, zig64 )
import           Avro.Name ( TypeName )
import           Avro.Value (Value)
import qualified Avro.Value as Value

import           Control.Monad (void)

import           Data.Binary (Put)
import qualified Data.Binary.Put as Put
import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.Int (Int32, Int64)
import           Data.Foldable (traverse_)
import           Data.Map (Map)
import qualified Data.Map as Map
import           Data.Maybe (listToMaybe)
import           Data.Text (Text)
import qualified Data.Text.Encoding as Text

import           GHC.Float (float2Double)


newtype Environment =
    Environment {
        getEnvironment :: Map TypeName (Get Value)
    }


emptyEnvironment :: Environment
emptyEnvironment =
    Environment Map.empty


environmentNames :: Environment -> [TypeName]
environmentNames (Environment env) =
    Map.keys env


insertEnvironment :: TypeName -> Get Value -> Environment -> Environment
insertEnvironment k v (Environment env) =
    Environment $
        Map.insert k v env

getZigZag32 :: Get Int32
getZigZag32 =
    zag32 <$>
        getVarInt


getZigZag64 :: Get Int64
getZigZag64 =
    zag64 <$>
        getVarInt



putZigZag32 :: Int32 -> Put
putZigZag32 =
    putVarInt . zig32


putZigZag64 :: Int64 -> Put
putZigZag64 =
    putVarInt . zig64



getPrefixedBytes :: Get ByteString
getPrefixedBytes = do
    len <- getZigZag32
    Get.getByteString (fromIntegral len)


getPrefixedUtf8 :: Get Text
getPrefixedUtf8 =
    Text.decodeUtf8 <$>
        getPrefixedBytes


getRecord :: Environment -> Map Int Value -> [ReadField] -> Get [Value]
getRecord env defaults fields =
    let
        go todo acc  =
            case todo of
                f : fs -> do
                    v <- makeDecoder env (ReadSchema.fldType f)
                    go fs $
                        maybe acc
                            (\p -> Map.insert p v acc)
                            (ReadSchema.fldPosition f)

                _ ->
                    pure (Map.elems acc)
    in
    go fields defaults


getBlocks :: (a -> b -> b) -> b -> Get a -> Get b
getBlocks cons empty element =
    let
        go state acc =
            if state > 0 then do
                e <- element
                go ( state - 1 ) ( cons e acc )

            else do
                b <- getZigZag32
                if b == 0 then
                    pure acc

                else if b < 0 then do
                    _ <- getZigZag32
                    go (negate b) acc

                else do
                    go b acc
    in
    go 0 empty


makeDecoder :: Environment -> ReadSchema -> Get Value
makeDecoder env schema =
    case schema of
        ReadSchema.Null ->
            pure Value.Null

        ReadSchema.Boolean ->
            (\b -> Value.Boolean (b >= 1)) <$>
                Get.getWord8

        ReadSchema.Int ->
            Value.Int <$>
                getZigZag32

        ReadSchema.IntAsLong ->
            Value.Long . fromIntegral <$>
                getZigZag32

        ReadSchema.IntAsFloat ->
            Value.Float . fromIntegral <$>
                getZigZag32

        ReadSchema.IntAsDouble ->
            Value.Double . fromIntegral <$>
                getZigZag32

        ReadSchema.Long ->
            Value.Long <$> getZigZag64

        ReadSchema.LongAsFloat ->
            Value.Float . fromIntegral <$>
                getZigZag64

        ReadSchema.LongAsDouble ->
            Value.Double . fromIntegral <$>
                getZigZag64

        ReadSchema.Float ->
            Value.Float <$>
                Get.getFloatle

        ReadSchema.FloatAsDouble ->
            Value.Double . float2Double <$>
                Get.getFloatle

        ReadSchema.Double ->
            Value.Double <$>
                Get.getDoublele

        ReadSchema.Bytes ->
            Value.Bytes <$>
                getPrefixedBytes

        ReadSchema.String ->
            Value.String <$>
                getPrefixedUtf8

        ReadSchema.Array items ->
            Value.Array . reverse <$>
                getBlocks (:) [] (makeDecoder env items)

        ReadSchema.Map values ->
            Value.Map  <$>
                getBlocks (uncurry Map.insert)
                    Map.empty
                    ((,) <$> getPrefixedUtf8 <*> makeDecoder env values)

        ReadSchema.Record name fields defaults ->
            let
                runRecord =
                    Value.Record <$>
                        getRecord (insertEnvironment name runRecord env) defaults fields
            in
            runRecord

        ReadSchema.Union options -> do
            discriminator <- getZigZag32
            case index (fromIntegral discriminator) options of
                Just ( ix, s ) ->
                    Value.Union ix <$>
                        makeDecoder env s

                Nothing ->
                    fail "Union payload discriminator unexpected"

        ReadSchema.AsUnion ix inner ->
            Value.Union ix <$>
                makeDecoder env inner

        ReadSchema.Enum _ symbols -> do
            loc <- getZigZag32
            case index (fromIntegral loc) symbols of
                Just ix ->
                    pure (Value.Enum ix)

                Nothing ->
                    fail "Enum payload discriminator unexpected"

        ReadSchema.Fixed _ size ->
            Value.Fixed <$>
                Get.getByteString size

        ReadSchema.NamedType nt ->
            case Map.lookup nt (getEnvironment env) of
                Just discovered ->
                    discovered

                Nothing ->
                    fail "Could not recover type name"


index :: Int -> [a] -> Maybe a
index i xs =
    listToMaybe $
        Prelude.drop i xs


sizedString :: Text -> Put
sizedString s = do
    let bytes = Text.encodeUtf8 s
    putZigZag32 (fromIntegral (ByteString.length bytes))
    Put.putByteString bytes


encodeValue :: Value -> Put
encodeValue value =
    case value of
        Value.Null ->
            mempty

        Value.Boolean b ->
            if b then
                Put.putWord8 1

            else
                Put.putWord8 0

        Value.Int i ->
            putZigZag32 i

        Value.Long i ->
            putZigZag64 i

        Value.Float i ->
            Put.putFloatle i

        Value.Double i ->
            Put.putDoublele i

        Value.Bytes b -> do
            putZigZag32 (fromIntegral (ByteString.length b))
            Put.putByteString b


        Value.String s ->
            sizedString s

        Value.Array [] ->
            putZigZag32 0

        Value.Array xs -> do
            putZigZag32 (fromIntegral (length xs))
            traverse_ encodeValue xs
            putZigZag32 0

        Value.Map xs ->
            if Map.null xs then
                putZigZag32 0

            else do
                putZigZag32 (fromIntegral (Map.size xs))
                void $ Map.traverseWithKey (\k v -> do sizedString k; encodeValue v) xs
                putZigZag32 0

        Value.Record items ->
            traverse_ encodeValue items

        Value.Union ix item -> do
            putZigZag32 (fromIntegral ix)
            encodeValue item

        Value.Fixed bytes ->
            Put.putByteString bytes

        Value.Enum ix ->
            putZigZag32 (fromIntegral ix)
