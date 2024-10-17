{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-| Definitions and helpers for Avro Names.

Record, Enum and Fixed types are /named/ types. Each has a full name that is composed of two parts;
a name and a namespace. Equality of names is defined on the full name.

A namespace is list of scoping names, encoded in the interface description language and Json specification
language as a dot separated string, but here as a list.

The empty string may also be used as a namespace to indicate the null namespace.
Equality of names (including field names and enum symbols) as well as fullnames is case-sensitive.

Record fields and enum symbols have names as well (but no namespace). Equality of fields and
enum symbols is defined on the name of the field \/ symbol within its scope (the record \/ enum that
defines it). Fields and enum symbols across scopes are never equal.

The name portion of the full name of named types, record field names, and enum symbols must:

  - start with @[A-Za-z\_]@
  - subsequently contain only @[A-Za-z0-9\_]@

-}
module Avro.Name
    ( TypeName (..)
    , contextualTypeName, canonicalName, renderFullName
    , validName, compatibleNames
    ) where

import qualified Data.Char as Char
import           Data.Function (on)
import qualified Data.List as List
import qualified Data.Maybe as Maybe
import           Data.Text (Text)
import qualified Data.Text as Text
import           Data.Foldable (traverse_)

{-| An Avro Type Name

This constructor is exposed, but one should only build names which
are correctly scoped and have valid names.

If unsure, one should use `contextualTypeName`
to parse the data instead.

-}
data TypeName =
  TypeName
    { baseName :: Text
    , nameSpace :: [Text]
    } deriving (Show)

instance Eq TypeName where
  (==) = (==) `on` renderFullName

instance Ord TypeName where
  compare = compare `on` renderFullName


{-| Render a TypeName.

This replaces short names with fullnames, using applicable namespaces
to do so, then eliminate namespace attributes, which are now redundant.

-}
renderFullName :: TypeName -> Text
renderFullName TypeName { baseName, nameSpace} =
    Text.intercalate "." (nameSpace ++ [baseName])


{-| Normalise a TypeName.

This replaces short names with fullnames, using applicable namespaces
to do so, then eliminate namespace attributes, which are now redundant.

-}
canonicalName :: TypeName -> TypeName
canonicalName input =
    TypeName { baseName = renderFullName input, nameSpace = [] }



{-| Build a TypeName from a qualified string.
-}
parseFullName :: Text -> Either Text TypeName
parseFullName input =
    case unsnoc (splitNameParts input) of
        Just ( rest, base ) ->
            TypeName
                <$> validNamePart base
                <*> traverse validNamePart rest

        Nothing ->
            Left "Type names must contain non-empty valid name parts."



{-| Build a TypeName from using the name and namespace fields within a context.

Rules for this are specified in [the avro specification](https://avro.apache.org/docs/1.11.1/specification/#names).

Arguments:

  - Optional context (parent name)
  - Name
  - Optional Namespace

-}
contextualTypeName :: Maybe TypeName -> Text -> Maybe Text -> Either Text TypeName
contextualTypeName context input explicit =
    if Maybe.isNothing (Text.findIndex (== '.') input) then
        case explicit of
            Just ns ->
                TypeName
                    <$> validNamePart input
                    <*> traverse validNamePart (splitNameParts ns)

            Nothing ->
                TypeName
                    <$> validNamePart input
                    <*> Right (maybe [] nameSpace context)

    else
        parseFullName input


{-| Whether two names are compatible for Schema resolution.

This means that either the unqualified names match, or an alias matches
the fully qualified name.

-}
compatibleNames :: (TypeName, [TypeName]) -> TypeName -> Bool
compatibleNames (readerName, readerAliases) writerName =
    baseName readerName
        == baseName writerName
        || List.elem writerName readerAliases




unsnoc :: [b] -> Maybe ( [b], b )
unsnoc list =
    let
        step x z =
            Just $
                case z of
                    Nothing ->
                        ( [], x )

                    Just ( a, b ) ->
                        ( x : a, b )
    in
    List.foldr step Nothing list




validNamePart :: Text -> Either Text Text
validNamePart s =
    case Text.unpack s of
        c : cs ->
            if Char.isAlpha c && List.all Char.isAlphaNum cs then
                Right s

            else
                Left "Type name is not alpha-numeric"

        _ ->
            Left "Type name is empty"



{-| Test that a `TypeName` is valid

That is, test that

  - start with [A-Za-z\_][A-Za-z_]
  - subsequently contain only [A-Za-z0-9\_][A-Za-z0-9_]

-}
validName :: TypeName -> Either Text TypeName
validName input = do
    traverse_ validNamePart (baseName input : nameSpace input)
    pure input


{-| Split a name (or namespace)

Unfortunately, the Avro specification itself has issues
around names.

The filtering of empty lists is used to fixup some of
these.

-}
splitNameParts :: Text.Text -> [ Text.Text ]
splitNameParts input =
    if Text.null input then
        []

    else
        List.filter (not . Text.null) $
            Text.splitOn "." input

