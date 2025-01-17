# DeltaCAT Storage Model Specification (WORK IN PROGRESS)
This document describes the **Deltacat Metastore Model**, which manages a 
hierarchical set of objects:

- **Namespace**
- **Table**
- **Table Version**
- **Stream**
- **Partition**
- **Delta**

## Naming, Mutability, and Canonical Strings
All objects have an "id" field. For Table Versions, Streams, Deltas, and Partitions with "named immutable ids", the 'id' field must be manually assigned prior to commit.

Other objects types (Namespace, Table) have mutable names. For these, "id" is automatically set to a new uuid v4. 

Every object has a globally unique Locator. The key element of each Locator is a canonical string, which uniquely identifies the object within a catalog given its name. This canonical string also lets us map mutable object names and aliases back to the object's (immutable) id, as we will see later. 

The canonical string is composed of its parent's canonical string, plus its own canonical string elements (found in the `parts` field of the object's `name`, which is a multi-part LocatorName).

| **Object Type**    | **Primary Key**                          | **Mutability**  |
|---------------------|------------------------------------------|-----------------|
| **Namespace**       | `name`                                  | Mutable         |
| **Table**           | `name`                                  | Mutable         |
| **Table Version**   | `name`                                  | Immutable       |
| **Stream**          | `stream_id` + `format`                  | Immutable       |
| **Partition**       | `partition_id` + `partition_values`     | Immutable       |
| **Delta**           | `stream_position`                       | Immutable    

# Directory/File Structure
### Object Directories (Immutable id)
Every metastore object (Namespace, Table, TableVersion, etc.) has a root 
directory whose name is its **immutable ID** (i.e., a UUID or named immutable ID). 

For instance, a namespace root looks like:
```
${catalog_root}/${namespace_id}/
    └──rev/
        ...
    └──${id-table-1}/
        ...
    └──${id-table-2}/
        ...
```

Within this immutable Id directory, there is a child directory for each child object, and a revision directory which holds metadata describing the object across all historical revisions. 

**Child Object Directories**

The parent object has a single directory for each child object whose name is equal to the child object's immutable ID, and each child directory's contents have the same structure shown above.

**Revision Directory**

The **`rev/`** directory contains versioned metadata files named:
`<revision_number_padded_20_digits>_<txn_operation_type>_<txn_id>.mpk`

- `revision_number_padded_20_digits` is zero-padded, e.g., `00000000000000000001`.
- `txn_operation_type` is the type of operation a transaction applied to the object, which is either `create`, `update`, or `delete`.
- `txn_id` is the unique id of the transaction that created this revision file.

### Name Resolution Directories
Certain objects (like **Namespaces** and **Tables**) may have **mutable** names. 
To support object renames and alias name creation while keeping an Immutable Id, we create
a **Name Resolution Directory** to map the object's mutable name or alias back to its immutable ID. It has the following properties:

- The name of the directory is a SHA-1 **digest** of the **canonical string** of the associated object Locator. 
  (e.g., `sha1_hexdigest("MyNamespace")` or `sha1_hexdigest("MyNamespace|MyTable")`).
- Inside this **Name Resolution Directory**, there is a single **Name Mapping File** 
  (zero bytes) that references the **Immutable ID** directory.

**Name Mapping File**

The mutable name directory just contains a file which maps to its immutable directory. The format of this file is: <revision_number_padded_20_digits>_<txn_operation_type>_<txn_id>.<object_id>
Here, `object_id` is the name of the associated object's **Immutable ID** directory. Note that (except immutable ID) this is the same format used by metadata revision files, and the same process is employed to `create`, `update`, and `delete` name mappings.

### Transaction Directory
The transaction directory (${catalog_root}/txn) is a special directory in the catalog root which holds all successfully committed transactions. It contains one **Transaction Log File** per successful transaction recording transaction details.
```
${CATALOG_ROOT}/txn/
  |-{tx-1-id}
  |-{tx-2-id}
```

## Example 
Putting all of the above together, here is an example catalog directory with one namespace and one table
```
${CATALOG_ROOT}/txn/                        # Global TXN logs
   └──1737070822189-20444dca-7e3d-4dd2-af70-23b8bc890966  # txn log file (<txn-start-time>-<txn-uuid>)
${CATALOG_ROOT}/<namespace_digest>/         # Mutable Name Resolution Directory for Namespace (SHA-1 Hex Digest of "MyNamespace")
    └──<Name mapping file>
${CATALOG_ROOT}/<namespace_id>/             # Namespace id is a GUID 
   └── rev/
       └── <Revision files>
   └── <table_id>/
       └── rev/
           └── <revision files>
    └── <table_digest>/
        └──<Name mapping file>
```
