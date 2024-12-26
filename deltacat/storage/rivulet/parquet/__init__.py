# TODO later on this will be moved to a dedicated package
from deltacat.storage.rivulet.parquet.file_reader import ParquetFileReader
from deltacat.storage.rivulet.reader.reader_type_registrar import FileReaderRegistrar

FileReaderRegistrar.register_reader("parquet", ParquetFileReader)
