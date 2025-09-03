import logging
import os
import re

import setuptools

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.dirname(__file__)


def find_version(*paths):
    version_file_path = os.path.join(ROOT_DIR, *paths)
    with open(version_file_path) as file_stream:
        version_match = re.search(
            r"^__version__ = ['\"]([^'\"]*)['\"]", file_stream.read(), re.M
        )
        if version_match:
            return version_match.group(1)
        raise RuntimeError(f"Failed to find version at: {version_file_path}")


with open(os.path.join(ROOT_DIR, "README.md"), "r", encoding="utf-8") as fh:
    long_description = fh.read()


setuptools.setup(
    name="deltacat",
    version=find_version("deltacat", "__init__.py"),
    author="Ray Team",
    description="DeltaCAT is a portable Pythonic Data Lakehouse powered by Ray.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ray-project/deltacat",
    packages=setuptools.find_packages(where=".", include="deltacat*"),
    extras_require={
        "iceberg": [
            "pyiceberg[glue] >= 0.9.0",
            "pyiceberg[hive] >= 0.9.0",
            "pyiceberg[sql-sqlite] >= 0.9.0",
        ],
        "beam": [
            "apache-beam == 2.65.0",
        ],
        # separate s3fs from other AWS dependencies due to vastly increased
        # installation times when included (due to boto version conflicts)
        "s3fs": ["s3fs == 2025.3.2"],
    },
    install_requires=[
        # any changes here should also be reflected in requirements.txt
        # AWS
        "aws-embedded-metrics == 3.2.0",
        "boto3 ~= 1.34",
        # GCP
        "google-cloud-storage",
        "gcsfs == 2025.3.2",
        # Misc
        "daft == 0.4.15",  # run `make type-mappings` if you change this! See README-development.md
        "intervaltree == 3.1.0",
        "numpy == 1.22.4",
        "pandas == 2.2.3",  # run `make type-mappings` if you change this! See README-development.md
        "polars == 1.28.1",  # run `make type-mappings` if you change this! See README-development.md
        # upgrade to pyarrow 18.0.0 causes test
        # TestCompactionSession::test_compact_partition_when_incremental_then_rcf_stats_accurate to fail
        # due to input_inflation exceeding 1e-5
        # Daft requires pyarrow == 16.00. TODO discuss upgrade with Daft
        "pyarrow == 16.0.0",  # run `make type-mappings` if you change this! See README-development.md
        "pydantic!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.*,!=2.4.*,<3",
        "pymemcache == 4.0.0",
        "ray[default] == 2.46.0",  # run `make type-mappings`` if you change this! See README-development.md
        "tenacity == 8.2.3",
        "typing-extensions == 4.6.1",
        "redis == 5.0.0",
        "schedule == 1.2.0",
    ],
    setup_requires=["wheel"],
    package_data={
        "compute/metastats": ["*.yaml"],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
