# TODO later on this will be moved to a dedicated package
from deltacat.experimental.storage.rivulet.feather.file_reader import FeatherFileReader
from deltacat.experimental.storage.rivulet.reader.reader_type_registrar import (
    FileReaderRegistrar,
)

FileReaderRegistrar.register_reader("feather", FeatherFileReader)
