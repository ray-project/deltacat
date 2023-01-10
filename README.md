# DeltaCAT

DeltaCAT is a Pythonic Data Catalog powered by Ray.

Its data storage model allows you to define and manage fast, scalable,
ACID-compliant data catalogs through git-like stage/commit APIs, and has been
used to successfully host exabyte-scale enterprise data lakes.

DeltaCAT uses the Ray distributed compute framework together with Apache Arrow
for common table management tasks, including petabyte-scale
change-data-capture, data consistency checks, and table repair.

## Getting Started

### Prerequisites
Install all developer dependencies
```
pip install -r dev-requirements.txt
```
### Setting up pre-commit
[`pre-commit`](https://pre-commit.com) is a framework for managing and maintaining multi-language pre-commit hooks:
###$ Prerequisites
1. Install pre-commit
```
pip install pre-commit
```
or
```
conda install -c conda-forge pre-commit
```
or just install the dev-requirements file.
```
pip install -r dev-requirements.txt
```

2. Install all mentioned hooks/packages in the `.pre-commit-config.yaml` file
```
pre-commit install
```

#### Usage
Test files modified by your commit
```
git add deltacat/__init__.py
git commit -m "this commit will be verified"
Trim Trailing Whitespace.................................................Passed
Fix End of Files.........................................................Passed
black....................................................................Passed
pycln....................................................................Passed
isort....................................................................Passed
...
```
#### Testing all files
For file-based checks, the only files checked are the ones modified by your commit. To  force pre-commit to check all files in your repo once run
```
pre-commit run --all-files
```

#### Skipping hooks
If you need to bypass the pre-commit hooks (not recommended), you can use the -n or --no-verify flag when committing.
```
git commit -nm "This commit should succeed"
```
