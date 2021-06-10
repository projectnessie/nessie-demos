# Datasets for demos

Since some notebook environments like Google Colaboratory only support uploading a single `.ipynb`
file, which cannot access "files next to it", the `pydemolib` code allows downloading
named datasets to the local file system. Files in a dataset can then be identified by name via a
Python `dict`.

# Uploading a new Dataset

A new dataset can be uploaded to a sub-directory of `datasets`. Please make sure that there is a `ls.txt`
file so that the code in `pydemolib` can quickly scan what files it should look at.
