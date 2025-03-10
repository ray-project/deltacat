import s3fs


def create_s3_file_system(s3_client_kwargs: dict) -> s3fs.S3FileSystem:
    if not s3_client_kwargs:
        return s3fs.S3FileSystem(anon=True)

    config_kwargs = {}
    if s3_client_kwargs.get("config") is not None:
        boto_config = s3_client_kwargs.pop("config")
        for key, val in boto_config.__dict__.items():
            if not key.startswith("_") and val is not None:
                config_kwargs[key] = val

    anon = False
    if s3_client_kwargs.get("aws_access_key_id") is None:
        anon = True

    return s3fs.S3FileSystem(
        anon=anon, client_kwargs=s3_client_kwargs, config_kwargs=config_kwargs or None
    )
