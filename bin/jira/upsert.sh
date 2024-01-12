#!/bin/bash
# A script to create, or update an existing Jira if it exists, for a given Summary
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv TOKEN PROJECT_KEY PULL_REQUEST SUMMARY TYPE

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
  cat << EOF | tee create-jira.json
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
  export ISSUE_KEY=$(echo ${ISSUES} | jq -r .issues[0].key)
  export PULL_REQUEST

  echo "Updating existing Jira ${ISSUE_KEY}"
  ${SCRIPT_DIR}/add_pull_request.sh
fi

export JIRA_TICKET_URL="${BASE_URL}/browse/${ISSUE_KEY}"
