from deltacat.storage.rivulet.fs.file_provider import FileProvider
from deltacat.storage.rivulet.metastore.sst import SSTableRow
from deltacat.storage.rivulet.reader.data_reader import FileReader
from typing import Type, Dict

from deltacat.storage.rivulet.schema.schema import Schema


class FileReaderRegistrar:
    """
    Registrar for readers of rivulet data

    Readers must adhere to the Protocol DataReader

    Packages with extension classes should call into this registrar in __init__.py
    """

    _readers = {}

    @classmethod
    def register_reader(
        cls,
        extension: str,
        reader_class: Type[FileReader],
        allow_overwrite: bool = False,
    ):
        """
        Register a file extension associated with a dataset reader

        Parameters:
        - extension: str, the file extension to register
        - reader_class: Type[DataReader], the reader class to associate with the extension
        - allow_overwrite: bool, if True, allows overwriting an existing reader for the extension
        """
        if extension in cls._readers and not allow_overwrite:
            raise ValueError(
                f"Reader for extension '{extension}' is already registered. "
                f"Set allow_overwrite=True to replace the existing reader."
            )
        normalized_extension = extension.lower()
        cls._readers[normalized_extension] = reader_class

    @classmethod
    def get_reader_class(cls, uri: str) -> Type[FileReader]:
        """
        Gets the reader class given a URI

        :param uri: URI of file to be read. Note that we expect the URI to end in a file extension
        :raises ValueError: if no registered data reader is found for the URI's extension type
        """
        # Find the file extension from the URI
        extension = uri.split(".")[-1].lower()

        # Return the reader class if the extension is registered, otherwise return None
        return cls._readers.get(extension)

    @classmethod
    def construct_reader_instance(
        cls,
        sst_row: SSTableRow,
        file_provider: FileProvider,
        primary_key: str,
        schema: Schema,
        reader_cache: Dict[str, FileReader] = None,
    ) -> FileReader:
        """
        Construct a data reader for an instance of a given uri

        :param uri: URI of file to be read. Note that we expect the URI to end in a file extension
        :param reader_cache: Optional cache of readers keyed by extension
        :raises ValueError: if no registered data reader is found for the URI's extension type
        """
        extension = sst_row.uri.split(".")[-1].lower()

        if reader_cache is not None and extension in reader_cache:
            return reader_cache[extension]

        reader_class = FileReaderRegistrar.get_reader_class(sst_row.uri)
        reader_instance = reader_class(sst_row, file_provider, primary_key, schema)

        if reader_cache:
            reader_cache[extension] = reader_instance

        return reader_instance
