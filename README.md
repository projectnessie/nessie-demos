# Prototype repo to build demos for Nessie

[![Main CI](https://github.com/snazy/nessie-demos/actions/workflows/main.yml/badge.svg)](https://github.com/snazy/nessie-demos/actions/workflows/main.yml)

This git repo is a temporary repo, which will eventually be retired and maybe deleted. The results
of this work will either go into the [Project Nessie repo](https://github.com/projectnessie/nessie/)
or a separate repo under [Project Nessie](https://github.com/projectnessie/).

## Scope

Build a framework for demos that run on [Google Colaboratory](https://colab.research.google.com/)
and [Katacoda](https://katacoda.com), and, as long as it is feasible and doesn't delay the work
for demos, have a base for production-like performance testing, which has the same or at least
similar restrictions/requirements as the demos.

Every demo must work, we want to eliminate manual testing, so all demos must be unit-testable.

There is also [Binder](https://binder.org/), which provides a pretty easy way to share notebooks
using a single link. Binder's is more flexible than Colaborary:
* Allows frontends like "plain notebook", Jupyter Lab, nteract, RStudio.
* Builds the runtime environment based on dependency files via a "tailor made" Docker image. But
  the cost is that the repo structure "must work" with Binder.
* Binder's default runtime environment is "pretty default", but allows different Python versions
  and probably also exotic sets of dependencies.
* Binder.org is supported by Google and runs on GKE.
* Maybe helpful: [Spark PR](https://github.com/apache/spark/pull/29491) that introduced Binder
  in Apache Spark
* Jupyter Notebook kernels shutdown pretty quickly after a short time of inactivity, which feels
  like a downside.

## Version restrictions

Nessie and for example Apache Iceberg need to be compatible to each other and must be based
on released versions. This means, that for example Iceberg 0.11.1 declares
[Nessie 0.3.0](https://github.com/apache/iceberg/blob/apache-iceberg-0.11.1/versions.props#L21),
but Nessie 0.4.0 and 0.5.1 work as well.

Apache Spark 3.0 is supported in Iceberg 0.11, Spark 3.1 is only supported since Iceberg 0.12. 

It's very likely that we have to change the Nessie code base to adjust for example the pynessie
dependencies, because of dependency issues in the hosted runtime of Colaboratory notebooks.

## Version flexibility

Different demos probably require different dependencies, like different versions of Apache Spark,
of Apache Iceberg, of Nessie, etc. This is why the set of dependencies in
[`requirements.txt`](setup/requirements.txt) is pretty short and quite relaxed.

Config files in the `configs/` directory contain various sets of dependencies. The nessiedemo-setup
code takes care or installing the Python dependencies, Apache Spark and the nessie-runner.

## Datasets for demos

Since some notebooks environments like Google Colaboratory only support uploading a single `.ipynb`
file, which cannot access "files next to it", the nessiedemo-setup code allows downloading
named datasets to the local file system. Files in a dataset can then be identified by name via a
Python `dict`.

## Build

Since demos must be unit-testable, we will need some build tool and GitHub workflows.

## Bumping versions of Nessie and Iceberg et al

Bumping versions should eventually work via a pull-request. I.e. a human changes the versions
used by the demos, submits a PR, CI runs against that PR, review, merge.

At least in the beginning, Nessie will evolve a lot, and we should expect a lot of changes
required to the demos for each version bump. For example, the current versions require a
"huge context switch" in the Demos: jumping from running Python code to executing binaries
and/or running SQL vs. executing binaries to perform tasks against Nessie.

It feels nicer to ensure that everything in the demos repo works against the "latest & greatest"
versions.

If someone wants to run demos against an older "set of versions":
* In Google Colaboratory it's as easy as opening a different URL.
* In Katakoda there seems to be no way to "just use a different URL/branch/tag".

We might either accept that certain environments just don't support "changing versions on the fly"
or we use a different strategy, if that's necessary. So the options are probably:
* Demos (in Katakoda) only work against the "latest set of versions"
* "Archive" certain, relevant demos in the demo-repo's "main" branch in separate directories

I suspect, this chapter requires some more "brain cycles".

### Preparing version bumps

It would be nice to prepare PRs for Nessie and Iceberg version bumps before those are actually
released.

Maybe we can prepare this convenient and "nice to have" ability.

It might also help to have this ability to run demos (and production-like perf tests) as a
"pre-release quality gate" to ensure that user-facing stuff works and there are no performance
regressions.

## Git history

The history in Git should work the same way as for [Project Nessie repo](https://github.com/projectnessie/nessie/),
i.e. a linear git history (no merges, no branches, PR-squash-and-merge).

## Compatibility Matrix

| Current | Nessie | Apache Iceberg | Apache Spark | Notes
| ------- | ------ | -------------- | ------------ | -----
| No |0.4.0 | 0.11.1 | 3.0 | Iceberg declares Nessie 0.3.0, but there are no (REST)API changes between Nessie 0.3.0 and 0.4.0
| **Yes** |0.5.1 | 0.11.1 | 3.0 | Iceberg declares Nessie 0.3.0, but there are no _breaking_ (REST)API changes between Nessie 0.4.0 and 0.5.1
| No |0.5.1 | (recent HEAD of `master` branch) | 3.1 |
| No |(recent HEAD of `main` branch) | n/a | n/a | **incompatible**, would require a build from a developer's branch
