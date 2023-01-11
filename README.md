# DeltaCAT

DeltaCAT is a Pythonic Data Catalog powered by Ray.

Its data storage model allows you to define and manage fast, scalable,
ACID-compliant data catalogs through git-like stage/commit APIs, and has been
used to successfully host exabyte-scale enterprise data lakes.

DeltaCAT uses the Ray distributed compute framework together with Apache Arrow
for common table management tasks, including petabyte-scale
change-data-capture, data consistency checks, and table repair.

## Getting Started
---
### Install
```
pip install deltacat
```
