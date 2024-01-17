#!/bin/bash
# A script to update a Jira tickets linked Pull Requests
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv TOKEN ISSUE_KEY ASSIGNEE

${CURL} -X PUT ${API_URL}/issue/${ISSUE_KEY}/assignee \
  -H @headers \
  --data "{\"name\":\"${ASSIGNEE}\"}"
