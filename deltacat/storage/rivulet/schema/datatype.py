# Similar to daft's datatype, this is a big ole enum of all possible types
#   In the long term, this will have to be interoperable with pandas/daft/spark/parquet/iceberg/etc type systems
#   Our Spec will need to publish data type mappings, such as Iceberg's data type mappings: https://iceberg.apache.org/spec/#file-system-operations
#   It also has the unique responsibility of representing multi-modal (e.g. image) types
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa


# OPEN QUESTIONS:
# Do we want to support the notion of logical vs physical type like parquet?

# TODO turn into an interface or otherwise allow pluggable datatypes
@dataclass(frozen=True)
class Datatype:
    type_name: str

    @property
    def subtype(self) -> Optional[str]:
        """
        Higher level formats like binary or image will have "subtype", such as image(jpg) or binary(np_array)
        TODO - Note that we are replacing this schema system with DeltaCat schema model, which supports extended/decorated pyarrow types
        For now going to do a super minimal/hacky implementation of types like binary and image, where
        :return: Subtype if it exists, or None
        """
        if not self.type_name.endswith(")"):
            return None
        if self.type_name.startswith("binary(") or self.type_name.startswith("image("):
            return self.type_name[self.type_name.find("(") + 1 : -1]
        return None

    @classmethod
    def binary(cls, binary_format):
        """
        :param binary_format:
        :return:
        """
        return cls(type_name=f"binary({binary_format})")

    @classmethod
    def image(cls, image_format):
        return cls(type_name=f"image({image_format})")

    @classmethod
    def string(cls):
        return cls(type_name="string")

    @classmethod
    def float(cls):
        return cls(type_name="float")

    @classmethod
    def int16(cls):
        return cls(type_name="int16")

    @classmethod
    def int32(cls):
        return cls(type_name="int32")

    @classmethod
    def int64(cls):
        return cls(type_name="int64")

    @classmethod
    def bool(cls):
        return cls(type_name="bool")

    @classmethod
    def from_pyarrow(cls, pa_type: pa.DataType) -> "Datatype":
        """
        Convert a pa type to a Rivulet Datatype.

        Args:
            pa_type: pa DataType to convert

        Returns:
            Datatype: Corresponding Rivulet Datatype

        Raises:
            ValueError: If the pa type is not supported
        """
        if pa.types.is_string(pa_type):
            return cls.string()
        elif pa.types.is_float64(pa_type):
            return cls.float()
        elif pa.types.is_int16(pa_type):
            return cls.int16()
        elif pa.types.is_int32(pa_type):
            return cls.int32()
        elif pa.types.is_int64(pa_type):
            return cls.int64()
        elif pa.types.is_boolean(pa_type):
            return cls.bool()
        elif pa.types.is_binary(pa_type):
            # TODO: Use pyarrow metadata on schema field to map correctly into image and other binary types
            return cls.binary("binary")  # Default binary format
        else:
            raise ValueError(f"Unsupported pa type: {pa_type}")

    def to_pyarrow(self) -> pa.field:
        """
        In the future we want to be more thoughtful about how we do type conversions

        For now, just build a simple mapping of every time to pyarrow
        For what it's worth, Daft schema types have a giant if/else like this

        :return: pyarrow type
        """
        if self.type_name == "string":
            return pa.string()
        elif self.type_name == "float":
            return pa.float64()
        elif self.type_name == "int16":
            return pa.int16()
        elif self.type_name == "int32":
            return pa.int32()
        elif self.type_name == "int64":
            return pa.int64()
        elif self.type_name == "bool":
            return pa.bool_()
        elif self.type_name.startswith("image(") or self.type_name.startswith(
            "binary("
        ):
            # TODO we will need to think about how custom types work with tabular libraries
            return pa.binary()
        else:
            raise ValueError(f"Unsupported type conversion to pa: {self.type_name}")
