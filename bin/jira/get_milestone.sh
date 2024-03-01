#!/bin/bash
# A script to update a Jira tickets linked Pull Requests
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv TOKEN PROJECT_KEY MAJOR_VERSION

VERSIONS_URL="${API_URL}/project/${PROJECT_KEY}/versions"

# Return the oldest unreleased version for the project
curl ${VERSIONS_URL} \
  | jq -r ".[] | select(.released==false) | select(.name | startswith(\"${MAJOR_VERSION}\")) | .name" \
  | sort -V \
  | head -n1
