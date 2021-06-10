# Demos configuration files

Each released version configuration gets its own configuration file.
Patch versions may change within the version files without prior notice.

Configurations in the 'in-development/' folder are not for "production" notebooks,
but for Nessie developers.

## Version restrictions

Nessie and for example Apache Iceberg need to be compatible to each other and must be based
on released versions. This means, that for example Iceberg 0.11.1 declares
[Nessie 0.3.0](https://github.com/apache/iceberg/blob/apache-iceberg-0.11.1/versions.props#L21),
but Nessie 0.4.0 and 0.5.1 work as well.

Apache Spark 3.0 is supported in Iceberg 0.11, Spark 3.1 is only supported since Iceberg 0.12.

It's very likely that we have to change the Nessie code base to adjust for example the `pynessie`
dependencies, because of dependency issues in the hosted runtime of Colaboratory notebooks.

## Version flexibility

Different demos probably require different dependencies, like different versions of Apache Spark,
of Apache Iceberg, of Nessie, etc. This is why the set of dependencies in
[`requirements.txt`](pydemolib/requirements.txt) is pretty short and quite relaxed.

Config files in the `configs/` directory contain various sets of dependencies. The `pydemolib`
code takes care or installing the Python dependencies, Apache Spark and the nessie-runner.

It shall also be possible to run (or at least test) the demos against non-released versions
of Nessie and Iceberg, if that's possible "without a ton of code and complexity".