# Deltacat Storage Model Specification (WORK IN PROGRESS)
This document describes the **Deltacat Metastore Model**, which manages a 
hierarchical set of objects:

- **Namespace**
- **Table**
- **Table Version**
- **Stream**
- **Partition**
- **Delta**

## Naming, Mutability, and Canonical Strings
All objects have an "id" field. For Table Versions, Streams, Deltas, and Partitions with "named immutable ids", the user provided id sets the "id" field. 

Other objects types (Namespace, Table, Table Version) have mutable names. For these, "id" is set to a new uuid v4. 

Every object has a unique canonical string, which uniquely identifies the object given the mutable name(s) of the object. This canonical string is mappable to the object's (immutable) id, as we will see later. 

The canonical string is composed of its parent's canonical string, plus its own canonical string elements (found in the LocatorName `parts`).

| **Object Type**    | **Primary Key**                          | **Mutability**  |
|---------------------|------------------------------------------|-----------------|
| **Namespace**       | `name`                                  | Mutable         |
| **Table**           | `name`                                  | Mutable         |
| **Table Version**   | `name`                                  | Immutable       |
| **Stream**          | `stream_id` + `format`                  | Immutable       |
| **Partition**       | `partition_id` + `partition_values`     | Immutable       |
| **Delta**           | `stream_position`                       | Immutable    

# Directory/file structure
## Object Directories (Immutable id)
Every metastore object (Namespace, Table, TableVersion, etc.) has a root 
directory. 

named by its **immutable ID** (e.g., a UUID or a numeric ID). 
Inside that directory:

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
**Child object directories**

The parent object has a single directory for each child object directory. Those directories in turn use the structure described above.

**Revision Directory**
The **`rev/`** directory contains versioned metadata files named:
`<revision_number_padded_20_digits>_<txn_operation_type>_<txn_id>.mpk`

- `revision_number_padded_20_digits` is zero-padded, e.g., `00000000000000000001`.
- `txn_operation_type` is typically `create`, `update`, or `delete`.
- `txn_id` is a unique identifier for the transaction that performed this operation.

### Mutable Name Directories
Certain objects (like **Namespaces** and **Tables**) may have **mutable** names. 
To support renames while keeping an **immutable** ID, the code can create
a “digest-based” directory:

- The **digest** is a SHA-1 hash of the object’s **canonical locator** string 
  (e.g., `"MyNamespace"` or `"MyNamespace|MyTable"`).
- This directory name is simply that SHA-1 digest (hex-encoded).
- Inside this **mutable name directory**, there is a single **Name Mapping File** 
  (zero bytes) that references the **immutable ID** directory.

**Name Mapping File**

The mutable name directory just contains a file which maps to its immutable directory. The format of this file is: <revision_number_padded_20_digits>_<txn_operation_type>_<txn_id>.<object_id>

### Transaction Directory
The transaction directory (/txn) is a special directory in the catalog root which holds all successfully committed transactions. It contains one empty file per successful transaction.
```
${CATALOG_ROOT}/txn/
  |-{tx-1-id}
  |-{tx-2-id}
```
## Aliases
TODO

## Example 
Putting all of the above together, here is an example catalog directory with one namespace and one table
```
${CATALOG_ROOT}/txn/                        # Global TXN logs
   └──20444dca-7e3d-4dd2-af70-23b8bc890966  # Empty tx file (GUID) 
${CATALOG_ROOT}/<namespace_digest>/         # Mutable name directory for namespace. SHA-1 of "MyNamespace"
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