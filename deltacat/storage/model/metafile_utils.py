

def get_class(serialized_dict: dict):
    from deltacat.storage import Table, Namespace, TableVersion, Stream, Delta, Partition

    """
    Given a serialized dictionary of data, get the type of metafile class to instantiate

    TODO more robust implementation. Right now this relies on assumption that XLocator will only be present
    in class X, and is brittle to renames. On the other hand, this implementation does not require any marker fields to be persisted, and a regression
    will be quickly detected by test_metafile.io or other unit tests
    """
    if(serialized_dict.__contains__("tableLocator")):
        return Table
    elif serialized_dict.__contains__("namespaceLocator"):
        return Namespace
    elif serialized_dict.__contains__("tableVersionLocator"):
        return TableVersion
    elif serialized_dict.__contains__("partitionLocator"):
        return Partition
    elif serialized_dict.__contains__("streamLocator"):
        return Stream
    elif serialized_dict.__contains__("deltaLocator"):
        return Delta
    else:
        raise ValueError(f'Could not find metafile class from serialized form: ${serialized_dict}')
