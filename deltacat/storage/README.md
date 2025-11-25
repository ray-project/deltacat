# DeltaCAT Storage Model
This document briefly describes the **DeltaCAT Storage Model**, which manages a
hierarchical set of metadata objects within a **Catalog Root Directory**. DeltaCAT
catalogs can be rooted in any [filesystem supported by PyArrow](https://arrow.apache.org/docs/python/filesystems.html).

**Catalog Object Hierarchy:**
- **Namespace** (contains 0 or more unordered tables)
- **Table** (contains 1 or more ordered table versions)
- **Table Version** (contains 1 or more unordered data streams)
- **Stream** (contains 1 or more unordered data partitions)
- **Partition** (contains 0 or more ordered deltas)
- **Delta** (contains 1 or more ordered data files to insert, update, or delete)

## Catalog Version Files

When a DeltaCAT catalog is first initialized, an empty version file is created in the
catalog root directory. This file is used to track **which** version of DeltaCAT wrote
to the catalog, and **when** it started writing to the catalog.

**File Format**: `${CATALOG_ROOT}/version/<deltacat_version>.<starting_from>`

`<deltacat_version>`: Version of DeltaCAT that wrote the file, like `2.0.0`.

`<starting_from>`: Epoch time in nanoseconds when that version first initialized the catalog with write access, like `1758178659283916000`.

## Object Identity and Naming System
DeltaCAT separates **immutable data storage** from **mutable name resolution** to enable renames and
aliases without rewriting metadata files.

DeltaCAT uses a combination of **Immutable IDs** and human-readable **Aliases** to uniquely identify objects:

1. **ID**: Immutable unique system identifier used to locate the object internally.
2. **Alias**: Human-readable alias used to uniquely identify an object amongst its siblings.

### Object Identity Rules

| **Object Type**    | **Alias**                                | **ID**           | **Renameable** |
|---------------------|-----------------------------------------|------------------|----------------|
| **Namespace**       | `name`                                  | UUID             | ✅ Yes         |
| **Table**           | `name`                                  | UUID             | ✅ Yes         |
| **Table Version**   | `version`                               | `version`        | ❌ No          |
| **Stream**          | `format`                                | UUID             | ❌ No          |
| **Partition**       | `partition_values` + `scheme`           | UUID             | ❌ No          |
| **Delta**           | `stream_position`                       | `stream_position`| ❌ No          |

### Canonical Strings

Every object has a **Canonical String** derived from its alias that identifies it uniquely amongst its siblings.
SHA-1 hexdigests of canonical strings are used to create **Name Resolution Directories**.

**Format Examples:**
- Namespace: `"my_namespace"`
- Table: `"my_table"`
- Table Version: `"1"`
- Stream Format: `"deltacat"`
- Partition: `"[2025-01-01, NA]|e463da25-8679-44c7-9cf8-911d6404cee5"`
- Delta: `"42"`

# Directory & File Structure

DeltaCAT uses two types of directories to separate immutable data storage from mutable name resolution,
**Immutable ID Directories** and **Name Resolution Directories**.

## Immutable ID Directories (Data Storage)

**Purpose**: Metadata and child object storage.

**Location**: `${PARENT_DIRECTORY}/${immutable_id}/`

Every metadata object has a directory named after its **Immutable ID** (UUID or manual ID).
This directory structure **never changes**, even when objects are renamed.

```
${CATALOG_ROOT}/${namespace_uuid}/           # Namespace data directory
    ├── rev/                                 # Namespace metadata revisions
    │   ├── 00000000000000000001_create_<txn_id>.mpk
    │   └── 00000000000000000002_update_<txn_id>.mpk
    ├── ${table_uuid_1}/                     # Table 1 data directory
    │   ├── rev/                             # Table metadata revisions
    │   ├── ${table_version_id_1}/           # Table Version 1
    │   │   ├── rev/                         # Table Version metadata
    │   │   ├── ${stream_id_1}/              # Stream (e.g., Parquet format)
    │   │   │   ├── rev/                     # Stream metadata
    │   │   │   └── ${partition_id_1}/       # Partition
    │   │   │       ├── rev/                 # Partition metadata
    │   │   │       └── ${delta_id_1}/       # Delta (data change)
    │   │   │           └── rev/             # Delta metadata
    │   │   └── ${stream_id_2}/              # Another stream (e.g., Delta format)
    │   └── ${table_version_id_2}/           # Table Version 2
    └── ${table_uuid_2}/                     # Table 2 data directory
```

### Metadata Revision Files

Each `rev/` directory contains **Metadata Revision Files** stored in partitioned subdirectories using a **2-level exponential partition transform with base 1000**:

**Directory Structure**: `rev/<base^1>/<base^0>/<revision_file>`
- Level 1: Multiples of 1000000 (1000^2): `1000000/`, `2000000/`, `3000000/`...
- Level 2: Multiples of 1000 (1000^1): `1000/`, `2000/`, `3000/`...
- **Example**: Revision 1234567 → `rev/2000000/1235000/00000000000001234567_create_<txn_id>`

**File Format**: `<revision_number>_<operation>_<transaction_id>`
- `revision_number`: Zero-padded 20 digits (e.g., `00000000000000000001`)
- `operation`: `create`, `update`, or `delete`
- `transaction_id`: Unique transaction UUID
- **Content Type**: File contents are written using **MessagePack**

Simultaneous updates to the same `rev/` partition directory result in a concurrent modification conflict, but any number of different `rev/` partition directories may be modified simultaneously.

## Name Resolution Directories (Mutable Name Mapping)

**Purpose**: Map mutable names (Namespaces, Tables) to their immutable IDs.

**Location**: `${PARENT_DIRECTORY}/${sha1_hexdigest_of_canonical_string}/`

For objects with mutable names, DeltaCAT creates **Name Resolution Directories** to enable
efficient renames without moving data. The directory name is a **SHA-1 hexdigest** of the
object's **Canonical String**.

```
${CATALOG_ROOT}/<namespace_name_sha1>/       # Maps "my_namespace" → namespace_uuid
    └── 00000000000000000001_create_<txn_id>.<namespace_uuid>

${CATALOG_ROOT}/<namespace_uuid>/<table_name_sha1>/  # Maps "my_table" → table_uuid
    ├── 00000000000000000001_create_<txn_id>.<table_uuid>
    ├── 00000000000000000002_delete_<txn_id>.<table_uuid>  # Table renamed
    └── ...
```

Simultaneous updates to the same name resolution directory results in a concurrent modification conflict, but any number of different name resolution directories may be modified simultaneously.

### Name Mapping Files

**Purpose**: Track which immutable ID a mutable name currently points to

**File Format**: `<revision_number>_<operation>_<transaction_id>.<target_object_id>`

- **Zero-byte files**: The content is empty, the filename contains all information
- **File extension**: The target object's immutable ID (UUID)
- **Operations**:
  - `create`: Name now points to this object ID
  - `delete`: Name no longer points to this object ID (e.g., during rename)
  - `update`: Name mapping changed to point to another object ID

Notice that **Name Mapping Files** are effectively just empty **Metadata Revision Files** with a `<target_object_id>` extension.

### How Name Resolution Works

1. **Lookup**: Alias name → SHA-1 hexdigest → name resolution directory
2. **Find Latest**: Get the most recent non-DELETE revision file
3. **Extract ID**: The file extension is the target object's immutable ID
4. **Access Data**: Use the immutable ID to access the data directory

**Example Table Rename**:
```
# Before rename: "users" table within namespace
${CATALOG_ROOT}/<namespace_uuid>/<users_sha1>/
    └── 00000000000000000001_create_<txn_id>.<table_uuid>

# After rename: "users" → "customers"
${CATALOG_ROOT}/<namespace_uuid>/<users_sha1>/              # Old name
    ├── 00000000000000000001_create_<txn_id>.<table_uuid>
    └── 00000000000000000002_delete_<txn_id>.<table_uuid>     # Deleted

${CATALOG_ROOT}/<namespace_uuid>/<customers_sha1>/          # New name
    └── 00000000000000000001_create_<txn_id>.<table_uuid>   # Same table_uuid!
```

## Transaction Logs

**Purpose**: Track all successful, failed, running, and paused catalog transactions.

**Location**:
    - `${CATALOG_ROOT}/txn/success/`: successful transactions
    - `${CATALOG_ROOT}/txn/failed/`: failed transactions
    - `${CATALOG_ROOT}/txn/running/`: running transactions
    - `${CATALOG_ROOT}/txn/paused/`: paused transactions

**Sucess** and **Failed** transaction files are stored in directories partitioned by each transaction's **UTC start time**:

- **Directory Structure**: `txn/<status>/<year>/<month>/<day>/<hour>/<minute>/<transaction_id>`
    - Year: 4 digits (e.g., `2025/`)
    - Month: 2 digits (e.g., `01/`)
    - Day: 2 digits (e.g., `15/`)
    - Hour: 2 digits (e.g., `14/`)
    - Minute: 2 digits (e.g., `30/`)

**Transaction ID Format**: `<start_time_epoch_ns>_<uuid>`

```
${CATALOG_ROOT}/txn/
├── success/                           # Committed transactions (directories)
│   └── 2025/01/15/14/30/
│       ├── 1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8/
│       │   └── 1754104277286904000     # Transaction end time file
│       └── 1754104277294120000_cb30678d-4b46-48e3-a802-d349ebee59ae/
│           └── 1754104277296485000     # Transaction end time file
├── failed/                            # Failed transactions (files)
│   └── 2025/01/15/14/32/
│       ├── 1754104397123456000_a1b2c3d4-5e6f-7g8h-9i0j-k1l2m3n4o5p6
│       └── 1754104398234567000_b2c3d4e5-6f7g-8h9i-0j1k-l2m3n4o5p6q7
├── running/                           # Running transactions (files)
│   └── 1754104417123456000_c1d2e3f4-5g6h-7i8j-9k0l-m1n2o3p4q5r6
├── paused/                            # Paused transactions (files)
│   ├── 1754104437123456000_d1e2f3g4-5h6i-7j8k-9l0m-n1o2p3q4r5s6
│   └── 1754104438234567000_e2f3g4h5-6i7j-8k9l-0m1n-o2p3q4r5s6t7u8
```

**Transaction Log File Format by State**:
- **Success**: Transaction ID directories contain a single file with transaction metadata whose name is the transaction end time.
    - `<transaction_id>/<transaction_end_time_epoch_ns>`
- **Failed/Running/Paused**: Transaction ID files with transaction metadata.
    - `<transaction_id>`

DeltaCAT transactions rely on MVCC snapshot isolation, with conflicts isolated to concurrent operations against the same object ID. Each transaction log contains details about the operations performed by that transaction.

Transactions transition from RUNNING → SUCCESS/PAUSED/FAILED states, with the transaction stored in an equivalently named parent directory at `${CATALOG_ROOT}/txn/${state}/`.

## Runtime Environment Requirements

By default, `<txn_start_time>` is derived from the system clock epoch time in nanoseconds, and thus assumes that the host interacting with DeltaCAT provide **strong system clock accuracy guarantees**.

DeltaCAT's transaction system also assumes that the filesystem hosting the catalog root directory offers strong read-after-write consistency.

Taken together, these requirements make DeltaCAT safe to use on most major cloud computing hosts (e.g., EC2, GCE, Azure VMs) and storage systems (e.g., S3, GCS, Azure Blob Storage), but should typically only be used for testing/experimental purposes on your local laptop (e.g., due to potential system clock drift).

# Complete Example

Here's a complete example showing a catalog with namespace "sales" containing a table "orders" with a single delta written to it:

```
${CATALOG_ROOT}/
├── txn/                                     # Transaction logs (time-partitioned)
│   └── success/
│       └── 2025/01/15/14/30/                # Year/Month/Day/Hour/Minute UTC partitions
│           ├── 1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8/
│           │   └── 1754104277286904000       # Transaction end time file
│           └── 1754104277294120000_cb30678d-4b46-48e3-a802-d349ebee59ae/
│               └── 1754104277296485000       # Transaction end time file
│
├── ee4a794d7e59ba3486d9e0a024270dffa760ee03/   # "sales" → namespace_uuid
│   └── 00000000000000000001_create_1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8.2dfcb27d-23c5-424d-b6fe-55e79609a8f3
│
├── 2dfcb27d-23c5-424d-b6fe-55e79609a8f3/        # Namespace data (UUID)
│   ├── rev/                                     # Namespace metadata (exponentially partitioned)
│   │   └── 1000000/1000/                        # Level-1/Level-2 partition for revision 1
│   │       └── 00000000000000000001_create_1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8
│   ├── 797efd8d2cd3c859c3f498388e4761a8c1e51fda/   # "orders" → table_uuid
│   │   ├── 00000000000000000001_create_1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8.813226ae-94f1-46bf-95d5-e8a9660b5e11
│   │   └── 00000000000000000002_delete_1754104277286904000_cb30678d-4b46-48e3-a802-d349ebee59ae.813226ae-94f1-46bf-95d5-e8a9660b5e11
│   ├── add315af62be21dc6172bc55ee0430b712e2922f/   # "customer_orders" → same table_uuid
│   │   └── 00000000000000000001_create_1754104277286904000_cb30678d-4b46-48e3-a802-d349ebee59ae.813226ae-94f1-46bf-95d5-e8a9660b5e11
│   └── 813226ae-94f1-46bf-95d5-e8a9660b5e11/        # Table data (UUID)
│       ├── rev/                                     # Table metadata (exponentially partitioned)
│       │   └── 1000000/1000/                        # Level-1/Level-2 partition for revisions 1-2
│       │       ├── 00000000000000000001_create_1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8
│       │       └── 00000000000000000002_update_1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8
│       └── 1/                                       # Table Version 1
│           ├── rev/                                 # Table Version metadata (exponentially partitioned)
│           │   └── 1000000/1000/                    # Level-1/Level-2 partition for revision 1
│           │       └── 00000000000000000001_create_1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8
│           └── deltacat/                            # Stream (DeltaCAT format)
│               ├── rev/                             # Stream metadata (exponentially partitioned)
│               │   └── 1000000/1000/                # Level-1/Level-2 partition for revision 1
│               │       └── 00000000000000000001_create_1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8
│               └── default/                         # Default partition
│                   ├── rev/                         # Partition metadata (exponentially partitioned)
│                   │   └── 1000000/1000/            # Level-1/Level-2 partition for revision 1
│                   │       └── 00000000000000000001_create_1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8
│                   └── 0/                           # Delta 0
│                       └── rev/                     # Delta metadata (exponentially partitioned)
│                           └── 1000000/1000/        # Level-1/Level-2 partition for revision 1
│                               └── 00000000000000000001_create_1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8
```

**What Happened**? The table "orders" was created with a single delta written to it then renamed to "customer_orders".
- **Metafile revisions**: All metadata files are stored in exponentially partitioned directories (`1000000/1000/` for early revisions)
- **Transaction logs**: Transaction files are stored in time-based partitioned directories (`2025/01/15/14/30/`)
- **Table data directory**: `813226ae-94f1-46bf-95d5-e8a9660b5e11/` never moved
- **Name mappings**: The old name mapping got a DELETE revision, a new name mapping was created pointing to the same table UUID
- **Child objects**: All child objects (table versions, streams, etc.) remained unchanged in their immutable ID directories
