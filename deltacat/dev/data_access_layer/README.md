Data Access Layer is a layer that allows DeltaCat and Daft to read and write to various table formats

## Justification
We want to allow users to register various table formats in their DeltaCAT catalogs, even if they are not using the DeltaCAT native metastore / data format. 

Users can always have the option to use native catalogs and tooling to access tables that are not in DC format. However, there are a core set of capabilities which we want to support across multile table format types to enable machine learning and other use cases:

1. Ability to read and write data, both locally, through Daft (on ray), or in the future through other query engines.
2. Ability to "import"/"export"/"copy" between formats. For example, "copy" from a webdataset into a deltacat native dataset.   

In the long term, we will concurrently expand the capabilities of DC native datasets to synchronize to other stream formats. For instance, a dataset can be created in DC format, and on an ongoing basis the metafiles and/or data files can be synchronized so that an Iceberg table with equivalent data is maintained as of the latest completed transaction at synchronization start, modulo any feature incompatibilities.

## Approach
Data access has two interfaces, "storage" and "catalog". Pending discussion, we can merge these interfaces into DeltaCAT storage/catalog interfaces or make them separate, pending more discussion. 

The lower level Storage data access layer is meant to integrate with Daft and Ray Data. 

The higher level Catalog interfaces provides convenient functions to read and write data, or to copy data between formats. 

TODO more detail

