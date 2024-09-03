module Avro.Internal.ReadSchema where

import Avro.Name (TypeName)
import Avro.Value (Value)

import Data.Map (Map)
import Data.Primitive.Array (Array)
import Data.Text (Text)


data ReadField =
    ReadField
        { fldName :: Text
        , fldType :: ReadSchema
        , fldPosition :: Maybe Int
        }


data ReadSchema
    = Null
    | Boolean
    | Int
    | IntAsLong
    | IntAsFloat
    | IntAsDouble
    | Long
    | LongAsFloat
    | LongAsDouble
    | Float
    | FloatAsDouble
    | Double
    | Bytes
    | String
    | Array ReadSchema
    | Map ReadSchema
    | NamedType TypeName
    | Record TypeName [ReadField] (Map Int Value)
    | Enum TypeName (Array Int)
    | Union (Array (Int, ReadSchema))
    | Fixed TypeName Int
    | AsUnion Int ReadSchema
