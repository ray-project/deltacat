from typing import Optional, Dict, Any
from pyiceberg.table import Table
from pyiceberg.catalog import Catalog, load_catalog as pyiceberg_load_catalog
from botocore.credentials import Credentials
import boto3
from boto3.session import Session


def load_catalog(
    iceberg_catalog_name: str, iceberg_catalog_properties: Dict[str, Any]
) -> Catalog:
    catalog = pyiceberg_load_catalog(
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


def get_bucket_name() -> str:
    return "test-bucket"


def get_s3_prefix() -> str:
    return get_s3_path(get_bucket_name())


def get_credential() -> Credentials:
    boto3_session: Session = boto3.Session()
    credentials: Credentials = boto3_session.get_credentials()
    return credentials


def get_glue_catalog() -> Catalog:
    credential = get_credential()
    # Credentials are refreshable, so accessing your access key / secret key
    # separately can lead to a race condition. Use this to get an actual matched
    # set.
    credential = credential.get_frozen_credentials()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    s3_path = get_s3_prefix()
    glue_catalog = pyiceberg_load_catalog(
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


def load_table(catalog: Catalog, table_name: str) -> Table:
    loaded_table = catalog.load_table(table_name)
    return loaded_table
