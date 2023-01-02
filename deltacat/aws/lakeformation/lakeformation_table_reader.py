import logging
import time
import datetime
import ray
import pyarrow
from typing import Optional, Callable, TypeVar
from ray.data.impl.block_list import BlockList
from ray.data.block import BlockMetadata
from deltacat.aws.lakeformation.lakeformation_utils import ( 
    get_lakeformation_client,
    merge_blocklists
)

T = TypeVar('T')

logger = logging.getLogger(__name__)

@ray.remote
def get_work_unit_result(region: str,
                        query_id: str, work_unit_id: int, work_unit_token: str,
                        mapper: Callable[[pyarrow.Table, str, str, dict], T],
                        role_arn: str, **kwargs) -> T:

    client = get_lakeformation_client(region, role_arn)
    response = client.get_work_unit_results(
        QueryId = query_id,
        WorkUnitId = work_unit_id,
        WorkUnitToken = work_unit_token
    )

    with pyarrow.ipc.open_stream(response["ResultStream"]._raw_stream) as reader:
        downloaded_pyarrow_table = reader.read_all()

    mapped_result = mapper(downloaded_pyarrow_table, query_id, work_unit_id, **kwargs)
    return mapped_result

class LakeformationTableReader:

    def __init__(self, catalog_id: str, database_name: str, region: str, table_name: str, role_arn: Optional[str] = None):
        """
        IAM Role arn is used to create the lakeformation client with Role's credentials. If not given creates a standard client.
        """
        self.catalog_id = catalog_id
        self.database_name = database_name
        self.region = region
        self.table_name = table_name
        self.role_arn = role_arn
        self.lakeformation_client = get_lakeformation_client(region= self.region, role_arn= self.role_arn)

    def read_table(self,  mapper: Callable[[pyarrow.Table, str, str, dict], T], batch_size: int, **kwargs) -> BlockList:
        """
        For the given lakeformation table, returns a BlockList containing object reference of mapped result for the whole table.
        All work units are processed parallely
        All the keyword arguments to this function are passed to the mapper.
        """
        start_time = datetime.datetime.now()
        query_id = self.__start_query_planning(table_name= self.table_name, query_as_of_time = start_time) 
        blocklist = self.__read_work_units(query_id, batch_size, mapper, **kwargs)
        
        return blocklist

    def __start_query_planning(self, table_name: str, query_as_of_time: int) -> None:
        SELECT_ALL_QUERY = 'SELECT * FROM \"{}\"'
        query_string = SELECT_ALL_QUERY.format(table_name)

        try:
            response = self.lakeformation_client.start_query_planning(
                QueryPlanningContext = {
                    'CatalogId': self.catalog_id,
                    'DatabaseName': self.database_name,
                    'QueryAsOfTime': query_as_of_time
                },
                QueryString = query_string
            )
            query_id = response['QueryId']
            
            self.__get_query_id_available(query_id)

            logger.info(f'QueryId for CatalogueID: {self.catalog_id}, database name: {self.database_name} is {query_id}')
            return query_id

        except self.lakeformation_client.exceptions.InvalidInputException as e:
            logger.info(f'Invalid Input Exception for CatalogueID: {self.catalog_id}, database name: {self.database_name}. {e}')
            raise e
    

    def __read_work_units(self, query_id:str, batch_size: int,
                        mapper: Callable[[pyarrow.Table, str, str], T], **kwargs) -> BlockList:
        next_token = ""
        blocklists = []

        while True:
            response_page = self.lakeformation_client.get_work_units(
                QueryId = query_id,
                PageSize = batch_size,
                NextToken = next_token
            )
            next_token = response_page["NextToken"]

            if not next_token:
                break
            
            for work_unit_range in response_page['WorkUnitRanges']:
                blocklists.append(self.__get_work_units_result(query_id, work_unit_range, mapper, **kwargs))

        merged_blocklist = merge_blocklists(blocklists)
        return merged_blocklist

    def __get_query_id_available(self, query_id = str):
        state = ""
        iteration = 0
        while(state != "FINISHED" and state != "ERROR"):
            iteration += 1
            time.sleep(0.5)
            response = self.lakeformation_client.get_query_state(
                QueryId = query_id
            )

            state = response['State']
            if(iteration > 240):
                raise RuntimeError(f"Could not get query id in final state, Query Id still in state: {state}") 
            

        if(state == "ERROR"):
            raise RuntimeError(f"Query Id : {query_id} moved to Error state")

        return state

    def __get_work_units_result(self, query_id: str, work_unit_range: dict,
                            mapper: Callable[[pyarrow.Table, str, str], T], **kwargs) -> BlockList:
                            
        result_promises, block_metadatas = [], []
        work_units_id_min: int= work_unit_range['WorkUnitIdMin']
        work_units_id_max: int = work_unit_range['WorkUnitIdMax']
        work_unit_token = work_unit_range['WorkUnitToken']

        for work_unit_id in range(work_units_id_min, work_units_id_max + 1):
            result_promises.append(get_work_unit_result.remote(self.region, query_id, work_unit_id, work_unit_token, mapper, self.role_arn, **kwargs))
            block_metadatas.append(BlockMetadata(None, None, None, [], None))
            
        return BlockList(result_promises, block_metadatas)

