# DeltaCAT Storage Model
This document briefly describes the **DeltaCAT Storage Model**, which manages a
hierarchical set of metadata objects within a **Catalog Root Directory**. DeltaCAT
catalogs can be rooted in any filesystem supported by PyArrow.

**Catalog Object Hierarchy:**
- **Namespace** (contains 0 or more unordered tables)
- **Table** (contains 1 or more ordered table versions)
- **Table Version** (contains 1 or more unordered data streams)
- **Stream** (contains 1 or more unordered data partitions)
- **Partition** (contains 0 or more ordered deltas)
- **Delta** (contains 1 or more ordered data files to insert, update, or delete)

## Object Identity and Naming System
DeltaCAT separates **immutable data storage** from **mutable name resolution** to enable renames and
aliases without rewriting metadata files.

DeltaCAT uses a two-layer system for object identity:

1. **ID**: Immutable unique identifier used internally to locate the object.
2. **Alias**: Alias used to find the object amongst siblings in the same level of the catalog object hierarchy.

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

Every object has a **Canonical String** that uniquely identifies its location in the hierarchy.
These strings are used to create SHA-1 digests for name resolution directories.

**Format Examples:**
- Namespace: `"my_namespace"`
- Table: `"<namespace_sha1_hexdigest>|<table_name>"`
- Table Version: `"<table_sha1_hexdigest>|<table_version>"`

# Directory & File Structure

DeltaCAT uses two types of directories to separate immutable data storage from mutable name resolution:

## 1. Immutable ID Directories (Data Storage)

**Purpose**: Store metadata and child objects using permanent IDs

**Location**: `${CATALOG_ROOT}/${object_immutable_id}/`

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

Each `rev/` directory contains **Metadata Revision Files** with transaction history:

**File Format**: `<revision_number>_<operation>_<transaction_id>.mpk`
- `revision_number`: Zero-padded 20 digits (e.g., `00000000000000000001`)
- `operation`: `create`, `update`, or `delete`
- `transaction_id`: Unique transaction UUID
- Extension: `.mpk` (MessagePack) or `.json` (if `METAFILE_FORMAT=json`)

## 2. Name Resolution Directories (Name Mapping)

**Purpose**: Map mutable names (Namespaces, Tables) to their immutable IDs
**Location**: `${CATALOG_ROOT}/${sha1_hexdigest_of_canonical_string}/`

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

### Name Mapping Files

**Purpose**: Track which immutable ID a mutable name currently points to
**File Format**: `<revision_number>_<operation>_<transaction_id>.<target_object_id>`

- **Zero-byte files**: The content is empty, the filename contains all information
- **File extension**: The target object's immutable ID (UUID)
- **Operations**:
  - `create`: Name now points to this object ID
  - `delete`: Name no longer points to this object ID (e.g., during rename)
  - `update`: Name mapping changed to point to another object ID

### How Name Resolution Works

1. **Lookup**: Alias name → SHA-1 hexdigest → name resolution directory
2. **Find Latest**: Get the most recent non-DELETE revision file
3. **Extract ID**: The file extension is the target object's immutable ID
4. **Access Data**: Use the immutable ID to access the data directory

**Example Table Rename**:
```
# Before rename: "users" table
${CATALOG_ROOT}/<"users"_sha1>/
    └── 00000000000000000001_create_<txn_id>.<table_uuid>

# After rename: "users" → "customers"
${CATALOG_ROOT}/<"users"_sha1>/              # Old name
    ├── 00000000000000000001_create_<txn_id>.<table_uuid>
    └── 00000000000000000002_delete_<txn_id>.<table_uuid>  # Deleted

${CATALOG_ROOT}/<"customers"_sha1>/          # New name
    └── 00000000000000000001_create_<txn_id>.<table_uuid>  # Same table_uuid!
```

## 3. Committed Transaction Log Directory

**Purpose**: Track all committed transactions for consistency and recovery

**Location**: `${CATALOG_ROOT}/txn/success/`

```
${CATALOG_ROOT}/txn/
    └── success/
        ├── <txn_start_time>_<txn_uuid_1>     # Transaction log file 1
        ├── <txn_start_time>_<txn_uuid_2>     # Transaction log file 2
        └── ...
```

DeltaCAT transactions rely on MVCC snapshot isolation, with conflicts isolated to concurrent operations against the same object ID. Each transaction log file contains details about what operations were performed in that transaction.

Transactions transition from RUNNING → SUCCESS/FAILED/TIMEOUT states, with the transaction stored in an equivalently named parent directory at `${CATALOG_ROOT}/txn/${state}/`.

# Complete Example

Here's a complete example showing a catalog with namespace "sales" containing table "orders":

```
${CATALOG_ROOT}/
├── txn/                                     # Transaction logs
│   └── success/
│       ├── 1754104277284541000_241a88e2-2a73-4975-945c-5973323b82f8
│       └── 1754104277286904000_cb30678d-4b46-48e3-a802-d349ebee59ae
│
├── ee4a794d7e59ba3486d9e0a024270dffa760ee03/   # "sales" → namespace_uuid
│   └── 00000000000000000001_create_<txn_id>.2dfcb27d-23c5-424d-b6fe-55e79609a8f3
│
├── 2dfcb27d-23c5-424d-b6fe-55e79609a8f3/        # Namespace data (UUID)
│   ├── rev/                                     # Namespace metadata
│   │   └── 00000000000000000001_create_<txn_id>.mpk
│   ├── 797efd8d2cd3c859c3f498388e4761a8c1e51fda/   # "orders" → table_uuid
│   │   ├── 00000000000000000001_create_<txn_id>.813226ae-94f1-46bf-95d5-e8a9660b5e11
│   │   └── 00000000000000000002_delete_<txn_id>.813226ae-94f1-46bf-95d5-e8a9660b5e11
│   ├── add315af62be21dc6172bc55ee0430b712e2922f/   # "customer_orders" → same table_uuid
│   │   └── 00000000000000000001_create_<txn_id>.813226ae-94f1-46bf-95d5-e8a9660b5e11
│   └── 813226ae-94f1-46bf-95d5-e8a9660b5e11/        # Table data (UUID)
│       ├── rev/                                     # Table metadata
│       │   ├── 00000000000000000001_create_<txn_id>.mpk
│       │   └── 00000000000000000002_update_<txn_id>.mpk
│       └── 1/                                       # Table Version 1
│           ├── rev/                                 # Table Version metadata
│           │   └── 00000000000000000001_create_<txn_id>.mpk
│           └── deltacat/                            # Stream (DeltaCAT format)
│               ├── rev/                             # Stream metadata
│               │   └── 00000000000000000001_create_<txn_id>.mpk
│               └── default/                         # Default partition
│                   ├── rev/                         # Partition metadata
│                   │   └── 00000000000000000001_create_<txn_id>.mpk
│                   └── 0/                           # Delta 0
│                       └── rev/                     # Delta metadata
│                           └── 00000000000000000001_create_<txn_id>.mpk
```

**What happened**: The table "orders" was renamed to "customer_orders". Notice:
- The table data directory `813226ae-94f1-46bf-95d5-e8a9660b5e11/` never moved
- The old name mapping got a DELETE revision
- A new name mapping was created pointing to the same table UUID
- All child objects (table versions, streams, etc.) remained unchanged
