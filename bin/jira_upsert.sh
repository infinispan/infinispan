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

requiredEnv TOKEN PROJECT_KEY PULL_REQUEST SUMMARY TYPE

BASE_URL=https://issues.redhat.com
API_URL=${BASE_URL}/rest/api/2

cat << EOF > headers
Authorization: Bearer ${TOKEN}
Content-Type: application/json
EOF

PROJECT=$(${CURL} -H @headers $API_URL/project/${PROJECT_KEY})
PROJECT_ID=$(echo ${PROJECT} | jq -r .id)
ISSUE_TYPE_ID=$(echo ${PROJECT} | jq -r ".issueTypes[] | select(.name==\"${TYPE}\").id")

JQL="project = ${PROJECT_KEY} AND summary ~ '${SUMMARY}'"
# Search issues for existing Jira ticket
ISSUES=$(${CURL} ${API_URL}/search \
-G --data-urlencode "jql=${JQL}" \
-H @headers
)
TOTAL_ISSUES=$(echo ${ISSUES} | jq -r .total)
if [ ${TOTAL_ISSUES} -gt 1 ]; then
  echo "Multiple Jiras found in '${PROJECT}' with summary ~ '${SUMMARY}'"
  exit 1
elif [ ${TOTAL_ISSUES} == 0 ]; then
  echo "Existing Jira not found, creating a new one"
  cat << EOF > create-jira.json
  {
    "fields": {
      "project": {
        "id": "${PROJECT_ID}"
      },
      "summary": "${SUMMARY}",
      "description": "${DESCRIPTION}",
      "customfield_12310220": "${PULL_REQUEST}",
      "issuetype": {
        "id": "${ISSUE_TYPE_ID}"
      }
    }
  }
EOF
  # We retry on error here as for some reason the Jira server occasionally responds with 400 errors
  ISSUE_KEY=$(${CURL} --retry 5 --retry-all-errors -H @headers --data @create-jira.json $API_URL/issue | jq -r .key)
else
  ISSUE=$(echo ${ISSUES} | jq .issues[0])
  ISSUE_KEY=$(echo ${ISSUE} | jq -r .key)

  echo "Updating existing Jira ${ISSUE_KEY}"

  EXISTING_PRS=$(echo ${ISSUE} | jq .fields.customfield_12310220)
  ALL_PRS="$(echo ${EXISTING_PRS} | jq '. + ["'${PULL_REQUEST}'"]' | jq -r '. |= join("\\n")')"
  echo $ALL_PRS

  cat << EOF > update-jira.json
  {
    "update": {
      "customfield_12310220": [
        {
          "set": "${ALL_PRS}"
        }
      ]
    }
  }
EOF

  # Add PR to existing issue
  ${CURL} -X PUT ${API_URL}/issue/${ISSUE_KEY} \
  -H @headers \
  --data @update-jira.json
fi

export JIRA_TICKET_URL="${BASE_URL}/browse/${ISSUE_KEY}"
