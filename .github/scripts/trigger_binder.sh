#!/bin/bash
#
# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# From https://github.com/scikit-hep/pyhf/blob/master/binder/trigger_binder.sh#7e64b9b599f3e014b622fb0082527e25379faab0 (Apache License 2)

function trigger_binder() {
    local URL="${1}"

    curl -L --connect-timeout 60 --max-time 600 "${URL}"
    curl_return=$?

    # Return code 28 is when the --max-time is reached
    if [ "${curl_return}" -eq 0 ] || [ "${curl_return}" -eq 28 ]; then
        if [[ "${curl_return}" -eq 28 ]]; then
            printf "\nBinder build started.\nCheck back soon.\n"
        fi
    else
        return "${curl_return}"
    fi

    return 0
}

trigger_binder "$@" || exit 1
