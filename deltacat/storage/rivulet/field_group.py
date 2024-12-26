from typing import Protocol, Dict, Any, List, runtime_checkable
from deltacat.storage.rivulet.glob_path import GlobPath
from deltacat.storage.rivulet.schema import Schema


@runtime_checkable
class FieldGroup(Protocol):
    """
    This represents a group of Fields (columns, nested data, multimodal) in a dataset.

    Like Datasets, Field groups have a Schema. They also have a primary key, which
        by convention will be an identifier associated to a multimodal asset

    TODO replace with much better interfaces for doing IO

    TODO FieldGroup is the right place for IO methods get and read

    In the future we need a dedicated IO interface with visitors for each field group. this way things like the GlobPathFieldGroup can be super simple (only holding URIs)
    """

    @property
    def schema(self) -> Schema:
        ...

    def save(self, path: str) -> None:
        return None


# Field group whose data is backed by a glob path
class GlobPathFieldGroup(FieldGroup):
    def __init__(self, glob_path: GlobPath, schema: Schema):
        self._glob_path = glob_path
        self._schema = schema

    @property
    def schema(self) -> Schema:
        return self._schema

    def __str__(self) -> str:
        return f"GlobPathFieldGroup(glob_path={self._glob_path}, schema={self._schema})"

    def __repr__(self) -> str:
        return self.__str__()


# Field group whose data is backed by a Python dictionary
class PydictFieldGroup(FieldGroup):
    def __init__(self, data: Dict[str, List[Any]], schema: Schema):
        self._col_data = data
        self._row_data: Dict[str, Dict[str, Any]] = {}
        self._schema = schema

        # build row level data
        if data:
            pk_col = data[schema.primary_key.name]
            for i, value in enumerate(pk_col):
                self._row_data[value] = {field_name: field_arr[i] for field_name, field_arr in data.items()}

    @property
    def schema(self) -> Schema:
        return self._schema

    def __str__(self) -> str:
        return f"DictFieldGroup(data={self._col_data}, schema={self._schema})"

    def __repr__(self) -> str:
        return self.__str__()


# Field group whose data is backed by the output of a MemTableDatasetWriter
class FileSystemFieldGroup(FieldGroup):
    def __init__(self, schema: Schema):
        self._schema = schema

    @property
    def schema(self) -> Schema:
        return self._schema

    def __str__(self) -> str:
        return f"FileSystemFieldGroup(schema={self._schema})"

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other):
        if not isinstance(other, FileSystemFieldGroup):
            return False
        return (self._schema) == (other._schema)
