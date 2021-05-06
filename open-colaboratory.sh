#!/usr/bin/env bash

set -e

FILE=$1
GIT_SHA="$(git rev-parse HEAD)"

if [[ ! -f ${FILE} || ! ${FILE} =~ .*[.]ipynb ]]; then
  echo "File argument ${FILE} is not a valid file"
fi

REL_FILEPATH="$(realpath --relative-to $(dirname $0) "${FILE}")"

URL="https://colab.research.google.com/github/snazy/nessie-demos/blob/${GIT_SHA}/${REL_FILEPATH}"

echo "Open ${URL}"
#case $(uname -s) in
#  Linux)
#    xdg-open ${URL}
#    ;;
#  Darwin)
#    open ${URL}
#    ;;
#esac
