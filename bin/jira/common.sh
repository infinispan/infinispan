#!/bin/bash
set -e -o pipefail
shopt -s expand_aliases


if [[ "$RUNNER_DEBUG" == "1" ]]; then
  set -x
  CURL_VERBOSE="--verbose"
fi

function requiredEnv() {
  for ENV in $@; do
      if [ -z "${!ENV}" ]; then
        echo "${ENV} variable must be set"
        exit 1
      fi
  done
}

alias curl="curl ${CURL_VERBOSE} --no-progress-meter --fail-with-body -H 'Authorization: Bearer ${TOKEN}' -H 'Content-Type: application/json'"

BASE_URL=https://issues.redhat.com
API_URL=${BASE_URL}/rest/api/2
