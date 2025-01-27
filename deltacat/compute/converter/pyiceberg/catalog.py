from typing import Optional


def load_catalog(iceberg_catalog_name, iceberg_catalog_properties):
    catalog = load_catalog(
        name=iceberg_catalog_name,
        **iceberg_catalog_properties,
    )
    return catalog


def get_s3_path(
    bucket_name: str,
    database_name: Optional[str] = None,
    table_name: Optional[str] = None,
) -> str:
    result_path = f"s3://{bucket_name}"
    if database_name is not None:
        result_path += f"/{database_name}.db"

    if table_name is not None:
        result_path += f"/{table_name}"
    return result_path


def get_bucket_name():
    return "metadata-py4j-zyiqin1"


def get_s3_prefix():
    return get_s3_path(get_bucket_name())


def get_credential():
    import boto3

    boto3_session = boto3.Session()
    credentials = boto3_session.get_credentials()
    return credentials


def get_glue_catalog():
    from pyiceberg.catalog import load_catalog

    credential = get_credential()
    # Credentials are refreshable, so accessing your access key / secret key
    # separately can lead to a race condition. Use this to get an actual matched
    # set.
    credential = credential.get_frozen_credentials()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    s3_path = get_s3_prefix()
    glue_catalog = load_catalog(
        "glue",
        **{
            "warehouse": s3_path,
            "type": "glue",
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret_access_key,
            "aws_session_token": session_token,
            "region_name": "us-east-1",
            "s3.access-key-id": access_key_id,
            "s3.secret-access-key": secret_access_key,
            "s3.session-token": session_token,
            "s3.region": "us-east-1",
        },
    )

    return glue_catalog


def load_table(catalog, table_name):
    loaded_table = catalog.load_table(table_name)
    return loaded_table
