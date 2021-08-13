# Nessie Binder Demos

These demos run under binder and can be found at:

* [Spark and Iceberg](https://mybinder.org/v2/gh/projectnessie/nessie-demos/main?filepath=notebooks/nessie-iceberg-demo-nba.ipynb)
* [Spark and Delta](https://mybinder.org/v2/gh/projectnessie/nessie-demos/main?filepath=notebooks/nessie-delta-demo-nba.ipynb)
* [Flink and Iceberg](https://mybinder.org/v2/gh/projectnessie/nessie-demos/main?filepath=notebooks/nessie-iceberg-flink-demo-nba.ipynb)

The are automatically rebuilt every time we push to master. They are unit tested using `testbook` library to ensure we get
the correct results as the underlying libraries continue to grow/mature.


## Upgrade instructions

Because of the split between Binder and unit tests it wasn't totally trivial to create a single place to update all versions.
Some versions have to be updated in multiple places:

### Nessie

Nessie version is set in Binder at `binder/requirements.txt` and for unit tests in `notebooks/tox.ini`. Currently, Iceberg and Delta
both support only 0.9.x of Nessie.

### Iceberg

Currently we are using Iceberg `0.12.0` and it is specified in both iceberg notebooks as well as `notebooks/tests/__init__.py`

### Delta

currently Delta version is taken directly from the Nessie version and isn't explicitly noted. It is currently `1.0.0-nessie`

### Spark

Only has to be updated in `binder/requirements.txt`. Currently Iceberg supports 3.0.x and 3.1.x while delta late supports
3.1.x only.

### Flink

Flink version is set in Binder at `binder/postBuild` and for unit tests in `notebooks/tox.ini`. Currently, Iceberg supports
only 1.12.1

### Hadoop

Hadoop libs are used by flink and currently specified in `notebooks/tests/__init__.py` only. We use 2.10.1 with Flink.
