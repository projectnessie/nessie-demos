#!/usr/bin/env bash

cd $(dirname $0)
. ../venv/bin/activate

for f in *.ipynb; do
  jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace $f
done
