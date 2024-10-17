# Avro Simple

[Apache Avro™ ](https://avro.apache.org/) support for Haskell.

Apache Avro™ is a leading serialisation format for record data,
with a powerful type system, and great support for schema evolution.

This library offers comprehensive support for reading and writing
Avro binary data to Haskell types, through the definition of
Codecs. These describe Avro Schemas, as well
as encoders and decoders for Avro values.

This project is a direct port of our Elm Avro library. As such, it
is not fancy.

But it is:

- Efficient,
- Highly conformant, and
- Easy to use.


As a simple example, below we define an Haskell record, and then build
a binary Codec for it.

```haskell
import           Avro
import           Avro.Codec (Codec)
import qualified Avro.Codec as Codec
import           Avro.Name (TypeName(..))

import           Data.Binary.Get (Get)
import           Data.Binary.Put (Put)
import           Data.Text (Text)
import           Data.Int (Int32)

data Person =
    Person {
        personWho :: Text
      , personAge :: Int32
    } deriving (Eq, Show)


personCodec :: Codec Person
personCodec =
    Codec.record (TypeName "person" []) $
        Person
            <$> Codec.requiredField "name" Codec.string personWho
            <*> Codec.requiredField "age" Codec.int personAge


{-| A byte encoder for a person.
-}
encoder :: Person -> Put
encoder =
    Avro.makeEncoder personCodec


{-| Build a decoder for data written using a schema.
-}
decoder :: Schema -> Either SchemaMismatch (Get Person)
decoder writerSchema =
    Avro.makeDecoder personCodec writerSchema
```


Acknowledgements and Alternatives
---------------------------------

While this code is derived from our Elm library, we would like to thank Haskell Works for
their [Avro library](https://github.com/haskell-works/avro), which inspires parts of the
implementation and clarified a number of points. It is an alternative well worth considering
for parsing Avro data in Haskell.

The main differences between this library and that are:

- Haskell Works' Avro library prefers type classes over values for encoders and decoders;
- It favours code generation using Template Haskell over writing decoders and encoders; and
- It doesn't have anything like our `Codec` type.

You should consider our library if:

- You are happy to write your own model types and codecs (which in turn generate schemas);
- You want to dynamically generate Avro data of unknown shape in your program (types defined
  by your users); or
- You are happy with a smaller library with fewer features but greater test coverage and
  excellent adherence to the Avro specification.

You should use Haskell works library for now if:

- You need to use the single container objects
- You want to generate your Haskell types from Avro schemas.
