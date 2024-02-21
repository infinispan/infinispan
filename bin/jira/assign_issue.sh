#!/bin/bash
# A script to update a Jira issues assignee
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv TOKEN ISSUE_KEY ASSIGNEE

curl -X PUT ${API_URL}/issue/${ISSUE_KEY}/assignee --data "{\"name\":\"${ASSIGNEE}\"}"
