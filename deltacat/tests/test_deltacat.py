import shutil
import tempfile
import deltacat as dc

from deltacat.env import create_ray_runtime_environment


class TestDeltaCAT:
    @classmethod
    def setup_class(cls):
        cls.temp_dir_1 = tempfile.mkdtemp()
        cls.temp_dir_2 = tempfile.mkdtemp()
        # Initialize DeltaCAT with two local catalogs.
        dc.init(
            catalogs={
                "test_catalog_1": dc.Catalog(root=cls.temp_dir_1),
                "test_catalog_2": dc.Catalog(root=cls.temp_dir_2),
            },
        )

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir_1)
        shutil.rmtree(cls.temp_dir_2)

    def test_cross_catalog_namespace_copy(self):
        # Given two empty DeltaCAT catalogs.
        # When the same namespace is created in both catalogs.
        namespace_src = dc.create_namespace(
            namespace="test_namespace",
            catalog="test_catalog_1",
        )
        namespace_dst = dc.create_namespace(
            namespace=namespace_src.namespace,
            properties=namespace_src.properties,
            catalog="test_catalog_2",
        )

        # Expect the catalog namespace created in each catalog
        # method to be equivalent and equal to the source namespace.
        assert namespace_src.equivalent_to(namespace_dst)
        assert namespace_src == namespace_dst

        # When each catalog namespace is fetched explicitly
        # Expect them to be equivalent but not equal
        # (due to different metafile IDs).
        actual_namespace_src = dc.get_namespace(
            namespace=namespace_src.namespace,
            catalog="test_catalog_1",
        )
        actual_namespace_dst = dc.get_namespace(
            namespace=namespace_dst.namespace,
            catalog="test_catalog_2",
        )
        assert actual_namespace_src.equivalent_to(actual_namespace_dst)
        assert not actual_namespace_src == actual_namespace_dst
