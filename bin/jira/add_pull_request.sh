#!/bin/bash
# A script to update a Jira tickets linked Pull Requests
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv TOKEN ISSUE_KEY PULL_REQUEST

ISSUE_URL="${API_URL}/issue/${ISSUE_KEY}"
ISSUE=$(${CURL} -H @headers ${ISSUE_URL} | jq .)
EXISTING_PRS=$(echo ${ISSUE} | jq .fields.customfield_12310220)
PULL_REQUESTS="$(echo ${EXISTING_PRS} | jq '. + ["'${PULL_REQUEST}'"]' | jq -r '. |= join("\\n")')"

cat << EOF | tee update-jira.json
{
  "update": {
    "customfield_12310220": [
      {
        "set": "${PULL_REQUESTS}"
      }
    ]
  }
}
EOF

${CURL} -X PUT ${ISSUE_URL} \
  -H @headers \
  --data @update-jira.json
