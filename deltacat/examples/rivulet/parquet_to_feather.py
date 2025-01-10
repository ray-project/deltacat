import pyarrow as pa
import pyarrow.parquet as pq
import deltacat as dc

# Step 1: Create a simple 3x3 Parquet file using pyarrow
parquet_file_path = "../contacts.parquet"
data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
}
table = pa.Table.from_pydict(data)
pq.write_table(table, parquet_file_path)

# Step 2: Load the Parquet file into a Dataset
dataset = dc.Dataset.from_parquet(
    name="contacts",
    file_uri=parquet_file_path,
    metadata_uri=".",
    merge_keys="id"
)

# Step 3: Add two new columns to the Dataset
dataset.add_fields([
    ("email", dc.Datatype.string()),
    ("is_active", dc.Datatype.bool())
])

# Step 4: Append two new records, including values for the new columns. The cool thing with deltacat datasets is
#         that deltacat will not attempt to rewrite the existing parquet file, they will store additional data files
#         alongside the original parquet file(s) that can be easily joined w/ the originals.

# Open a new writer that will write new data to feather files
dataset_writer = dataset.writer(file_format="feather")

# Define some new rows w/ the expanded schema and write them
new_rows = [
    {
        "id": 4,
        "name": "David",
        "age": 40,
        "email": "david@example.com",
        "is_active": True
    },
    {
        "id": 5,
        "name": "Eve",
        "age": 45,
        "email": "eve@example.com",
        "is_active": False
    }
]
dataset_writer.write(new_rows)

# Write into the new columns on existing rows and write them, again without modifying/messing with the original parquet file.
new_columns_existing_rows = [
    {
        "id": 3,
        "email": "charlie@example.com",
        "is_active": True
    },
    {
        "id": 2,
        "email": "bob@example.com",
        "is_active": False
    },
    {
        "id": 1,
        "email": "alice@example.com",
        "is_active": False
    }
]
dataset_writer.write(new_columns_existing_rows)

# Write dataset data/metadata into feather files.
dataset_writer.flush()

# Step 5 (Optional): Read data from feather file.
#read_feather = feather.read_feather('./.riv-meta-contacts/data/<replace with generated file>')
#print(read_feather)