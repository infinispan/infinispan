#!/bin/bash
# A script to update a Jira tickets linked Pull Requests
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv TOKEN ISSUE_KEY PULL_REQUEST

ISSUE_URL="${API_URL}/issue/${ISSUE_KEY}"
ISSUE=$(curl ${ISSUE_URL} | jq .)
EXISTING_PRS=$(echo ${ISSUE} | jq .fields.customfield_12310220)

if [[ ${EXISTING_PRS} == *"${PULL_REQUEST}"* ]]; then
  echo "Pull Request '${PULL_REQUEST}' already linked to ${ISSUE_KEY}"
  exit
fi

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

curl -X PUT ${ISSUE_URL} --data @update-jira.json
