from __future__ import annotations

from deltacat.storage.rivulet.parquet.serializer import ParquetDataSerializer
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.serializer import DataSerializer
from deltacat.storage.rivulet.fs.file_provider import FileProvider

from deltacat.storage.rivulet.feather.serializer import FeatherDataSerializer


class DataSerializerFactory:
    """
    Simple factory class for getting the appropriate serializer given a schema
    TODO make this more modular/pluggable like DatasetReaderRegistrar
      This will be more challenging to make pluggable, because we should not rely on a simple 1:1 mapping of type to serializer
      The actual logic for determining how to serialize a given schema may be complex
      e.g.: if schema contains datatype X, you must use serializer Y. Otherwise, default to serializer Z
    """

    @classmethod
    def get_serializer(
        self,
        schema: Schema,
        file_provider: FileProvider,
        user_provided_format: str | None = None,
    ) -> DataSerializer:
        if user_provided_format == "parquet":
            return ParquetDataSerializer(file_provider, schema)
        elif user_provided_format == "feather":
            return FeatherDataSerializer(file_provider, schema)
        elif user_provided_format is not None:
            raise ValueError("Unsupported format. Must be 'parquet' or 'feather'.")

        # Default engine logic. For now, if there is image or binary use feather
        has_binary_or_image = any(
            field.datatype.type_name.startswith(("binary", "image"))
            for field in schema.values()
        )
        if has_binary_or_image:
            return FeatherDataSerializer(file_provider, schema)
        else:
            return ParquetDataSerializer(file_provider, schema)
