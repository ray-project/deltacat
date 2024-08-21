import botocore.errorfactory
import pytest
import boto3
import botocore
from deltacat.storage import glue as gs
from moto import mock_glue, mock_lakeformation


class TestGlueStorage:

    TEST_PRINCIPAL = "arn:aws:iam::1234567890:role/test"

    def _create_database(self, database_name, glue, lf):
        glue.create_database(DatabaseInput={"Name": database_name})
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": self.TEST_PRINCIPAL},
            Resource={"Database": {"Name": "test"}},
            Permissions=["SELECT"],
        )

    def _create_table(self, table_name, database_name, glue, lf):
        try:
            self._create_database(database_name, glue, lf)
        except botocore.errorfactory.ClientError:
            pass
        glue.create_table(DatabaseName=database_name, TableInput={"Name": table_name})

    @mock_glue
    @mock_lakeformation
    def test_list_namespaces_sanity(self):
        assert gs.list_namespaces().all_items() == []

    @mock_glue
    @mock_lakeformation
    def test_list_namespaces_when_one_database(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")
        self._create_database("test", glue, lf)

        # action
        namespaces = gs.list_namespaces().all_items()

        # verify
        assert len(namespaces) == 1
        assert namespaces[0].namespace == "test"
        assert len(namespaces[0].permissions) == 1
        assert (
            namespaces[0].permissions[0]["Principal"]["DataLakePrincipalIdentifier"]
            == self.TEST_PRINCIPAL
        )

    @mock_glue
    @mock_lakeformation
    def test_list_namespaces_when_multiple_databases(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")

        for db_id in range(10):
            self._create_database(f"test-{db_id}", glue, lf)

        # action
        namespaces = gs.list_namespaces(MaxResults=3).all_items()

        # verify
        assert len(namespaces) == 10

    @mock_glue
    @mock_lakeformation
    def test_list_tables_when_database_absent(self):
        with pytest.raises(botocore.errorfactory.ClientError):
            gs.list_tables("test")

    @mock_glue
    @mock_lakeformation
    def test_list_tables_when_database_with_no_tables(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")
        self._create_database("test", glue, lf)

        # action
        tables = gs.list_tables("test").all_items()

        # verify
        assert tables == []

    @mock_glue
    @mock_lakeformation
    def test_list_tables_when_one_table(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")
        self._create_table("test-table", "test-namespace", glue, lf)

        # action
        tables = gs.list_tables("test-namespace").all_items()

        # verify
        assert len(tables) == 1
        assert tables[0].table_name == "test-table"
        assert tables[0].namespace == "test-namespace"

    @mock_glue
    @mock_lakeformation
    def test_list_tables_when_multiple_tables(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")
        for table_id in range(3):
            self._create_table(f"test-table-{table_id}", "test-namespace", glue, lf)

        # action
        tables = gs.list_tables("test-namespace", MaxResults=1).all_items()

        # verify
        assert len(tables) == 3
        assert tables[0].namespace == "test-namespace"

    @mock_glue
    @mock_lakeformation
    def test_get_namespace_when_database_absent(self):
        assert gs.get_namespace("test") is None

    @mock_glue
    @mock_lakeformation
    def test_get_namespace_when_database_present(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")
        self._create_database("test", glue, lf)

        # action
        result = gs.get_namespace("test")

        # verify
        assert result.namespace == "test"
        assert len(result.permissions) == 1

    @mock_glue
    @mock_lakeformation
    def test_get_table_when_table_absent(self):
        self._create_table
        assert gs.get_table("test-namespace", "test-table") is None

    @mock_glue
    @mock_lakeformation
    def test_get_table_when_table_present(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")
        self._create_table("test-table", "test-namespace", glue, lf)

        # action
        result = gs.get_table("test-namespace", "test-table")

        # verify
        assert result.table_name == "test-table"
        assert result.namespace == "test-namespace"
        assert len(result.permissions) == 1

    @mock_glue
    @mock_lakeformation
    def test_namespace_exists_when_database_absent(self):
        assert not gs.namespace_exists("test")

    @mock_glue
    @mock_lakeformation
    def test_namespace_exists_when_database_present(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")
        self._create_database("test", glue, lf)

        # action
        result = gs.namespace_exists("test")

        # verify
        assert result

    @mock_glue
    @mock_lakeformation
    def test_table_exists_when_table_absent(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")
        self._create_database("test-namespace", glue, lf)

        assert not gs.table_exists("test-namespace", "test-table")

    @mock_glue
    @mock_lakeformation
    def test_table_exists_when_table_present(self):
        glue = boto3.client("glue", "us-east-1")
        lf = boto3.client("lakeformation", "us-east-1")
        self._create_table("test-table", "test-namespace", glue, lf)

        # action
        result = gs.table_exists("test-namespace", "test-table")

        # verify
        assert result

    @mock_glue
    @mock_lakeformation
    def test_table_exists_when_namespace_absent(self):
        assert not gs.table_exists("test-namespace", "test-table")
