#!/bin/bash
# A script to transition a Jira ticket's state
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv TOKEN ISSUE_KEY TRANSITION

TRANSITIONS_URL="${API_URL}/issue/${ISSUE_KEY}/transitions"
TRANSITIONS=$(${CURL} -H @headers ${TRANSITIONS_URL} | jq .)
TRANSITION_ID=$(echo ${TRANSITIONS} | jq -r ".transitions[] | select(.name==\"${TRANSITION}\").id")

cat << EOF | tee update-jira.json
{
  "transition": {
    "id": "${TRANSITION_ID}"
  }
}
EOF

${CURL} -X POST "${TRANSITIONS_URL}" \
  -H @headers \
  --data @update-jira.json
