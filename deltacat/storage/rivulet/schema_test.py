@dataclass(frozen=True)
class Field:
    name: str
    datatype: Datatype
    is_merge_key: bool = False

class Schema(MutableMapping[str, Field]):
    def __init__(
        self,
        fields: Iterable[Tuple[str, Datatype] | Field] = None,
        merge_keys: Optional[Iterable[str]] = None,
    ):
        self._fields: Dict[str, Field] = {}
        merge_keys = merge_keys or {}
        if len(fields or []) == 0:
            if len(merge_keys) > 0:
                raise TypeError(
                    "It is invalid to specify merge keys when no fields are specified. Add fields or remove the merge keys."
                )
            return
        # Convert all input tuples to Field objects and add to fields
        for field in fields:
            if isinstance(field, tuple):
                name, datatype = field
                processed_field = Field(
                    name=name, datatype=datatype, is_merge_key=(name in merge_keys)
                )
            elif isinstance(field, Field):
                processed_field = field
                name = field.name
                # Check if merge key status conflicts
                if len(merge_keys) > 0:
                    expected_merge_key_status = name in merge_keys
                    if processed_field.is_merge_key != expected_merge_key_status:
                        raise TypeError(
                            f"Merge key status conflict for field '{name}': "
                            f"Provided as merge key: {expected_merge_key_status}, "
                            f"Field's current status: {processed_field.is_merge_key}. "
                            f"Merge keys should only be defined if raw (name, Datatype) tuples are used."
                        )
            else:
                raise TypeError(f"Unexpected field type: {type(field)}")
            self.add_field(processed_field)


import json
import tarfile
def to_field_from_dict(json_dict):
    """
    Convert a dictionary of key/value pairs into a list of 'field' dicts.
    """
    fields = []
    for k, v in json_dict.items():
        fields.append({
            'name': k,
            'datatype': str(type(v)),  # or just type(v)
            'is_merge_key': False
        })
    return fields
def process_tar(tar_path):
    """
    Opens the given tar file, looks for any *.json files inside,
    and for each JSON file, extracts its contents in-memory
    and prints the resulting fields.
    """
    with tarfile.open(tar_path, "r:*") as tar:
        for member in tar.getmembers():
            # Only process regular files that end with .json
            if member.isfile() and member.name.endswith(".json"):
                # Extract a file-like object from the archive
                f = tar.extractfile(member)
                if f:
                    # Load JSON directly from the file-like object
                    data = json.load(f)
                    # Convert the JSON dict to field objects
                    fields = to_field_from_dict(data)
                    print(f"Fields from {member.name}:")
                    print(fields)
                    print("----")
# Example usage:
# process_tar("/path/to/your/archive.tar")