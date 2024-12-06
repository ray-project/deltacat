import msgpack


def metafile(cls):
    """
    Decorator to add file read and write methods to dict-based DeltaCAT model
    classes. Uses msgpack (https://msgpack.org/) to serialize and deserialize
    metadata files.
    """

    def write(self, path: str):
        with open(path, "wb") as file:
            packed = msgpack.dumps(self)
            file.write(packed)

    def read(path: str):
        with open(path, "rb") as file:
            bytes = file.read()
        return cls(**msgpack.loads(bytes))

    cls.write = write
    cls.read = read

    return cls
