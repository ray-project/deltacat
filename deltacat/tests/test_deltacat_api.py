import shutil
import tempfile
import deltacat as dc


class TestDeltaCAT:
    @classmethod
    def setup_class(cls):
        cls.temp_dir_1 = tempfile.mkdtemp()
        cls.temp_dir_2 = tempfile.mkdtemp()
        # Initialize DeltaCAT with two local catalogs.
        dc.put("test_catalog_1", root=cls.temp_dir_1)
        dc.put("test_catalog_2", root=cls.temp_dir_2)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir_1)
        shutil.rmtree(cls.temp_dir_2)

    def test_cross_catalog_namespace_copy(self):
        # Given two empty DeltaCAT catalogs.
        # When a namespace is copied across catalogs.
        namespace_src = dc.put("test_catalog_1/test_namespace")
        namespace_dst = dc.copy(
            "test_catalog_1/test_namespace",
            "test_catalog_2",
        )
        # Expect the catalog namespace created in each catalog
        # method to be equivalent and equal to the source namespace.
        assert namespace_src.equivalent_to(namespace_dst)
        assert namespace_src == namespace_dst

        # When each catalog namespace is fetched explicitly
        # Expect them to be equivalent but not equal
        # (due to different metafile IDs).
        actual_namespace_src = dc.get("test_catalog_1/test_namespace")
        actual_namespace_dst = dc.get("test_catalog_2/test_namespace")
        assert actual_namespace_src.equivalent_to(actual_namespace_dst)
        assert not actual_namespace_src == actual_namespace_dst
