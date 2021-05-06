#!/usr/bin/env bash

set -e

FILE=$1
GIT_SHA="$(git rev-parse HEAD)"

if [[ ! -f ${FILE} || ! ${FILE} =~ .*[.]ipynb ]]; then
  echo "File argument ${FILE} is not a valid file"
fi

RELPATH="$(realpath --relative-to $(dirname $0) "${FILE}")"
FILEPATH_PARAM="$(echo "${RELPATH}" | sed 's/[/]/%2F/g')"

URL="https://mybinder.org/v2/gh/snazy/nessie-demos/${GIT_SHA}?filepath=${FILEPATH_PARAM}"

echo "Open ${URL}"
#case $(uname -s) in
#  Linux)
#    xdg-open ${URL}
#    ;;
#  Darwin)
#    open ${URL}
#    ;;
#esac
