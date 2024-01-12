#!/bin/bash
# A script to update a Jira tickets linked Pull Requests
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv TOKEN ISSUE_KEY VERSION

cat << EOF | tee update-jira.json
{
  "update": {
    "fixVersions": [
      {
        "add": {
          "name": "${VERSION}"
        }
      }
    ]
  }
}
EOF

${CURL} -X PUT ${API_URL}/issue/${ISSUE_KEY} \
  -H @headers \
  --data @update-jira.json
