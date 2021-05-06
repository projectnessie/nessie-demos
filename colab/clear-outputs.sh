#!/usr/bin/env bash

cd $(dirname $0)
. ../venv/bin/activate

if [[ -n $1 ]]; then
  PATTERN="$1"
else
  PATTERN="*.ipynb"
fi

for f in $PATTERN; do
  jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace $f
done
