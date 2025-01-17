# DeltaCAT Storage Model Specification (WORK IN PROGRESS)
This document describes the **DeltaCAT Metastore Model**, which manages a
hierarchical set of objects within a **Catalog Root Directory**:

- **Namespace**
- **Table**
- **Table Version**
- **Stream**
- **Partition**
- **Delta**

## Naming, Mutability, and Canonical Strings
All objects have an immutable `id` field. For **Table Versions**, **Streams**, **Deltas**, and **Partitions** with **Named Immutable IDs**, the `id` field must be manually assigned prior to commit.

Other objects types (**Namespace**, **Table**) have mutable names. For these, the `id` field is automatically set to a new UUID V4.

Every object has a globally unique `Locator`. The key element of each `Locator` is a **Canonical String**, which uniquely identifies the object within a catalog given its name. This **Canonical String** also lets us map mutable object names and aliases back to the object's **Immutable ID**, as we will see later.

The **Canonical String** is composed of its parent's **Canonical String**, plus its own **Canonical String** elements (found in the `parts` field of the object's `name`, which is a multi-part `LocatorName`).

| **Object Type**    | **Primary Key**                          | **Mutability**  |
|---------------------|------------------------------------------|-----------------|
| **Namespace**       | `name`                                  | Mutable         |
| **Table**           | `name`                                  | Mutable         |
| **Table Version**   | `version`                               | Immutable       |
| **Stream**          | `stream_id` + `format`                  | Immutable       |
| **Partition**       | `partition_id` + `partition_values`     | Immutable       |
| **Delta**           | `stream_position`                       | Immutable

# Directory & File Structure
### Object Directories (Immutable ID Directories)
Every metastore object (**Namespace**, **Table**, **TableVersion**, etc.) has a root
directory whose name is its **Immutable ID** (i.e., a UUID or **Named Immutable ID**).

For instance, a **Namespace** root looks like:
```
${CATALOG_ROOT}/${namespace_id}/
    └──rev/
        ...
    └──${id-table-1}/
        ...
    └──${id-table-2}/
        ...
```

Within this **Immutable ID Directory**, there is a child directory for each child object, and a **Revision Directory** which holds metadata describing the object across all historical revisions.

**Child Object Directories**

The parent object has a single directory for each child object whose name is equal to the child object's **Immutable ID**, and each child directory's contents have the same structure shown above.

**Revision Directory**

The **`rev/`** directory contains **Metadata Revision Files** which contain MessagePack-serialized representations of each object revision. The format of each **Metadata Revision File** is:
`<revision_number_padded_20_digits>_<txn_operation_type>_<txn_id>.mpk`

- `revision_number_padded_20_digits` is zero-padded, e.g., `00000000000000000001`.
- `txn_operation_type` is the type of operation a transaction applied to the object, which is either `create`, `update`, or `delete`.
- `txn_id` is the unique id of the transaction that created this revision file.

### Name Resolution Directories
Certain objects (like **Namespaces** and **Tables**) may have **mutable** names.
To support object renames and alias name creation while keeping an **Immutable ID**, we create
a **Name Resolution Directory** to map the object's mutable name or alias back to its **Immutable ID**. It has the following properties:

- The name of the directory is a **SHA-1 Digest** of the **Canonical String** of the associated object `Locator`.
  (e.g., `sha1_hexdigest("MyNamespace")` or `sha1_hexdigest("MyNamespace|MyTable")`).
- Inside this **Name Resolution Directory**, there is a single **Name Mapping File**
  (zero bytes) that references the **Immutable ID Directory**.

**Name Mapping File**

The format of the **Name Mapping File** file is: 
`<revision_number_padded_20_digits>_<txn_operation_type>_<txn_id>.<object_id>`
Where `object_id` is the name of the associated object's **Immutable ID** directory. 
Note that (except **Immutable ID**) this is the same format used by **Metadata Revision Files**, and the same process is employed to `create`, `update`, and `delete` name mappings.

### Transaction Log Directory
The **Transaction Log Directory** (`${CATALOG_ROOT}/txn`) is a special directory in the **Catalog Root Directory** which holds all successfully committed transactions. It contains one **Transaction Log File** per successful transaction recording transaction details.
```
${CATALOG_ROOT}/txn/
  |-${txn-1-start-time}_${txn-1-uuid}
  |-${txn-2-start-time}_${txn-2-uuid}
```

## Example
Putting all of the above together, here is an example catalog directory with one **Namespace** and one **Table**:
```
${CATALOG_ROOT}/txn/                        # Global TXN logs
   └──1737070822189-20444dca-7e3d-4dd2-af70-23b8bc890966  # txn log file (<txn-start-time>-<txn-uuid>)
${CATALOG_ROOT}/<namespace_digest>/         # Mutable Name Resolution Directory for Namespace (SHA-1 Hex Digest of "MyNamespace")
    └──<Name mapping file>
${CATALOG_ROOT}/<namespace_id>/             # Namespace ID is a UUID
   └── rev/
       └── <Revision files>
   └── <table_id>/
       └── rev/
           └── <revision files>
    └── <table_digest>/
        └──<Name mapping file>
```
