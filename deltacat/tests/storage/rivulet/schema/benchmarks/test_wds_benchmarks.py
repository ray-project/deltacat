import tarfile
import io
import json
import random
import pytest
from PIL import Image
from deltacat.storage.rivulet import Dataset


def create_test_webdataset_tar(path, num_entries=100):
    with tarfile.open(path, "w") as tar:
        for i in range(num_entries):
            filename = f"img_{i}.jpg"

            # Create synthetic image
            image = Image.new('RGB', (64, 64), color=(i % 255, 100, 100))
            img_bytes = io.BytesIO()
            image.save(img_bytes, format="JPEG")
            img_bytes.seek(0)

            img_info = tarfile.TarInfo(name=filename)
            img_info.size = len(img_bytes.getvalue())
            tar.addfile(img_info, img_bytes)

            # Create JSON metadata
            metadata = {
                "filename": filename,
                "label": random.randint(0, 9),
                "width": 64,
                "height": 64
            }
            json_bytes = io.BytesIO(json.dumps(metadata).encode("utf-8"))
            json_info = tarfile.TarInfo(name=f"{filename}.json")
            json_info.size = len(json_bytes.getvalue())
            tar.addfile(json_info, json_bytes)


@pytest.fixture(scope="module")
def synthetic_tar(tmp_path_factory):
    tar_path = tmp_path_factory.mktemp("data") / "synthetic.tar"
    create_test_webdataset_tar(tar_path, num_entries=500)
    return tar_path


@pytest.mark.benchmark(group="from_webdataset")
def test_from_webdataset_benchmark(tmp_path, benchmark):
    tar_path = "../../../../test_utils/resources/imagenet1k-train-0000.tar"

    def load_dataset():
        Dataset.from_webdataset(
            name="benchmark_webdataset",
            file_uri=tar_path,
            metadata_uri=tmp_path,
            merge_keys="filename",
            batch_size=8
        )

    benchmark(load_dataset)


@pytest.mark.benchmark(group="baseline_reader")
def test_baseline_json_reader_benchmark(benchmark):
    import pyarrow.json as pj
    tar_path = "../../../../test_utils/resources/imagenet1k-train-0000.tar"

    def read_json_only():
        with tarfile.open(tar_path, "r") as tar:
            for member in tar.getmembers():
                if member.name.endswith(".json"):
                    f = tar.extractfile(member)
                    if f:
                        pj.read_json(f)

    benchmark(read_json_only)


def test_synthetic_benchmark(tmp_path, synthetic_tar, benchmark):
    benchmark(lambda: Dataset.from_webdataset(
        name="synthetic_benchmark",
        file_uri=str(synthetic_tar),
        metadata_uri=tmp_path,
        merge_keys="filename"
    ))
