#!/bin/bash
set -e -o pipefail

CURL="curl --no-progress-meter --fail-with-body"
if [[ "$RUNNER_DEBUG" == "1" ]]; then
  set -x
  CURL="${CURL} --verbose"
fi

function requiredEnv() {
  for ENV in $@; do
      if [ -z "${!ENV}" ]; then
        echo "${ENV} variable must be set"
        exit 1
      fi
  done
}

BASE_URL=https://issues.redhat.com
API_URL=${BASE_URL}/rest/api/2

cat << EOF | tee headers
Authorization: Bearer ${TOKEN}
Content-Type: application/json
EOF
