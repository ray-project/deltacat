import boto3
from typing import Optional, List
from ray.data.impl.stats import DatasetStats
from ray.data.impl.block_list import BlockList


def get_lakeformation_client(region: str, role_arn: Optional[str] = None):
    """
    This method returns lakeformation client. 
    If role_arn is supplied; gets the lakeformation client for role arn's credentials. 
    """
    if role_arn:
        sts_client = boto3.client("sts") 
        _assumed_role = sts_client.assume_role( 
                RoleArn = role_arn, 
                RoleSessionName = "deltacat_lakeformation_session", 
            ) 
        credentials = _assumed_role["Credentials"] 
        access_key = credentials["AccessKeyId"] 
        secret_key = credentials["SecretAccessKey"] 
        session_token = credentials["SessionToken"]

        lf_client = boto3.client( 
                "lakeformation",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key, 
                aws_session_token=session_token, 
                region_name = region,
            )
        return lf_client

    else:
        return boto3.client("lakeformation", region)


def merge_blocklists(blocklists: List[BlockList]) -> BlockList:
    """
    merges list of blocklists into a single blocklist
    """

    merged_blocks = []
    merged_metadata = []

    for blocklist in blocklists:
        merged_blocks.extend(blocklist.get_blocks())
        merged_metadata.extend(blocklist.get_metadata())
    
    return BlockList(merged_blocks, merged_metadata)

