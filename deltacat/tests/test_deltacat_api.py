import shutil
import tempfile

import deltacat as dc
from deltacat.constants import METAFILE_FORMAT_MSGPACK
from deltacat import Namespace, DeltaCatUrl, DatasetType
from deltacat.storage import Metafile

from deltacat.io import (
    METAFILE_TYPE_COLUMN_NAME,
    METAFILE_DATA_COLUMN_NAME,
)


class TestDeltaCAT:
    @classmethod
    def setup_method(cls):
        cls.temp_dir_1 = tempfile.mkdtemp()
        cls.temp_dir_2 = tempfile.mkdtemp()
        # Initialize DeltaCAT with two local catalogs.
        dc.init()
        dc.put(DeltaCatUrl("dc://test_catalog_1"), root=cls.temp_dir_1)
        dc.put(DeltaCatUrl("dc://test_catalog_2"), root=cls.temp_dir_2)

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.temp_dir_1)
        shutil.rmtree(cls.temp_dir_2)

    def test_cross_catalog_namespace_copy(self):
        # Given two empty DeltaCAT catalogs.
        # When a namespace is copied across catalogs.
        namespace_src = dc.put(DeltaCatUrl("dc://test_catalog_1/test_namespace"))
        namespace_dst = dc.copy(
            DeltaCatUrl("dc://test_catalog_1/test_namespace"),
            DeltaCatUrl("dc://test_catalog_2/test_namespace"),
        )
        # Expect the catalog namespace created in each catalog
        # method to be equivalent and equal to the source namespace.
        assert namespace_src.equivalent_to(namespace_dst)
        assert namespace_src == namespace_dst

        # When each catalog namespace is fetched explicitly
        # Expect them to be equivalent but not equal
        # (due to different metafile IDs).
        actual_namespace_src = dc.get(DeltaCatUrl("dc://test_catalog_1/test_namespace"))
        actual_namespace_dst = dc.get(DeltaCatUrl("dc://test_catalog_2/test_namespace"))
        assert actual_namespace_src.equivalent_to(actual_namespace_dst)
        assert not actual_namespace_src == actual_namespace_dst

    def test_catalog_listing_shallow_local_metafiles(self):
        # Given two empty DeltaCAT catalogs.
        # When a namespace is put in the catalog.
        namespace_src: Namespace = dc.put(
            DeltaCatUrl("dc://test_catalog_1/test_namespace")
        )
        # Expect the namespace to be listed.
        assert any(
            namespace_src.equivalent_to(other)
            for other in dc.list(DeltaCatUrl("dc://test_catalog_1"))
        )

    def test_catalog_listing_shallow_ray_dataset(self):
        # Given two empty DeltaCAT catalogs.
        # When a namespace is put in the catalog.
        namespace_src: Namespace = dc.put(
            DeltaCatUrl("dc://test_catalog_1/test_namespace")
        )
        # Expect the namespace to be listed.
        dataset = dc.list(
            DeltaCatUrl("dc://test_catalog_1"),
            dataset_type=DatasetType.RAY_DATASET,
        )
        actual_namespace = Metafile.deserialize(
            serialized=dataset.take(1)[0][METAFILE_DATA_COLUMN_NAME],
            meta_format=METAFILE_FORMAT_MSGPACK,
        )
        assert actual_namespace.equivalent_to(namespace_src)
        namespace_type = dataset.take(1)[0][METAFILE_TYPE_COLUMN_NAME]
        assert namespace_type == "Namespace"
