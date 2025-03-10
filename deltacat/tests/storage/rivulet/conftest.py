import io

import pytest
from faker import Faker

from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.mvp.Table import MvpTable
from deltacat.storage.rivulet.schema.schema import Schema
import random
import string
from PIL import Image

FIXTURE_ROW_COUNT = 10000


@pytest.fixture
def ds1_dataset() -> MvpTable:
    """
    dataset with one million rows
    primary key is integer between 1 and 1,000,000

    TODO change to user Faker instead of int ranges
    """

    # Function to generate random names
    def generate_random_name():
        return "".join(
            random.choices(
                string.ascii_uppercase + string.ascii_lowercase, k=random.randint(3, 10)
            )
        )

    # Create a list of numbers from 1 to TEST_ROW_COUNT
    ids = list(range(1, FIXTURE_ROW_COUNT + 1))
    random.shuffle(ids)

    # Generate one million rows
    return MvpTable(
        {
            "id": ids,
            "name": [generate_random_name() for _ in range(FIXTURE_ROW_COUNT)],
            "age": [random.randint(18, 100) for _ in range(FIXTURE_ROW_COUNT)],
        }
    )


@pytest.fixture
def ds1_schema():
    return Schema(
        {
            ("id", Datatype.int32()),
            ("name", Datatype.string()),
            ("age", Datatype.int32()),
        },
        "id",
    )


@pytest.fixture
def ds2_dataset():
    """
    dataset2 with one million rows that can be joined to ds1
    primary key is integer between 1 and 1,000,000
    """
    # Create a list of numbers from 1 to 1,000,000
    ids = list(range(1, FIXTURE_ROW_COUNT + 1))
    random.shuffle(ids)

    fake = Faker()

    # Generate one million rows
    return MvpTable(
        {
            "id": ids,
            "address": [fake.address() for _ in range(FIXTURE_ROW_COUNT)],
            "zip": [fake.zipcode() for _ in range(FIXTURE_ROW_COUNT)],
        }
    )


@pytest.fixture
def ds2_schema():
    return Schema(
        {
            ("id", Datatype.int32()),
            ("address", Datatype.string()),
            ("zip", Datatype.string()),
        },
        "id",
    )


@pytest.fixture
def combined_schema(ds1_schema, ds2_schema):
    return Schema(
        {
            ("id", Datatype.int32()),
            ("address", Datatype.string()),
            ("zip", Datatype.string()),
            ("name", Datatype.string()),
            ("age", Datatype.int32()),
        },
        "id",
    )


@pytest.fixture
def dataset_images_with_label() -> (MvpTable, Schema):
    """
    dataset with one thousand images and labels, generated dynamically
    primary key is integer between 1 and 1,000
    """
    ROW_COUNT = 1000
    fake = Faker()
    schema = Schema(
        {
            ("id", Datatype.int32()),
            ("image", Datatype.image("jpg")),
            ("label", Datatype.string()),
        },
        "id",
    )

    # Create a list of numbers from 1 to ROW_COUNT
    ids = list(range(1, ROW_COUNT + 1))
    random.shuffle(ids)

    fake_image = Image.new(
        "RGB",
        (512, 512),
        color=(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)),
    )
    # get bytes from image encoded as png
    buffer = io.BytesIO()
    fake_image.save(buffer, format="PNG")
    # seek to start of buffer since we just wrote to it
    buffer.seek(0)
    image_bytes = buffer.read()
    # Generate one million rows
    return (
        MvpTable(
            {
                "id": ids,
                "image": [image_bytes for _ in range(ROW_COUNT)],
                "label": [fake.name() for _ in range(ROW_COUNT)],
            }
        ),
        schema,
    )
