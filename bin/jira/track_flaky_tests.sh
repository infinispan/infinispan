#!/bin/bash
set -e
# A script to track flaky tests in Jira
# Requires xmlstarlet and jq to be installed
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv TOKEN PROJECT_KEY TYPE JENKINS_JOB_URL FLAKY_TEST_GLOB

PROJECT=$(curl $API_URL/project/${PROJECT_KEY})
PROJECT_ID=$(echo ${PROJECT} | jq -r .id)
ISSUE_TYPE_ID=$(echo ${PROJECT} | jq -r ".issueTypes[] | select(.name==\"${TYPE}\").id")

shopt -s nullglob globstar
TESTS=(${FLAKY_TEST_GLOB})
for TEST in "${TESTS[@]}"; do
  TEST_CLASS=$(xmlstarlet sel --template --value-of '/testsuite/testcase/@classname' ${TEST})
  TEST_NAME=$(xmlstarlet sel --template --value-of '/testsuite/testcase/@name' ${TEST})
  TEST_NAME=${TEST_NAME% (Flaky Test)}
  TEST_NAME_NO_PARAMS=${TEST_NAME%%\\*}
  STACK_TRACE=$(xmlstarlet sel --template --value-of '/testsuite/testcase/failure' ${TEST})

  # Create Issue for Test Class+TestName
  SUMMARY="Flaky test: ${TEST_CLASS}#${TEST_NAME_NO_PARAMS}"
  echo ${SUMMARY}
  JQL="project = ${PROJECT_KEY} AND summary ~ '${SUMMARY}'"
  # Search issues for existing Jira issue
  ISSUES="$(curl ${API_URL}/search -G --data-urlencode "jql=${JQL}")"
  TOTAL_ISSUES=$(echo "${ISSUES}" | jq -r .total)
  if [ ${TOTAL_ISSUES} -gt 1 ]; then
    echo "Multiple Jiras found in '${PROJECT_KEY}' with summary ~ '${SUMMARY}'"
    exit 1
  fi

  if [ ${TOTAL_ISSUES} == 0 ]; then
    echo "Existing Jira not found, creating a new one"
    cat << EOF | tee create-jira.json
    {
      "fields": {
        "project": {
          "id": "${PROJECT_ID}"
        },
        "summary": "${SUMMARY}",
        "issuetype": {
          "id": "${ISSUE_TYPE_ID}"
        },
        "labels": [
          "flaky-test"
        ]
      }
    }
EOF
    # We retry on error here as for some reason the Jira server occasionally responds with 400 errors
    export ISSUE_KEY=$(curl --retry 5 --retry-all-errors --data @create-jira.json $API_URL/issue | jq -r .key)
  else
    export ISSUE_KEY=$(echo "${ISSUES}" | jq -r '.issues[0].key')
    # Re-open the issue if it was previously resolved
    TRANSITION="New" ${SCRIPT_DIR}/transition.sh
  fi

  COMMENT=$(
  cat << EOF
  h1. ${TEST_NAME}
  [Jenkins Job|${JENKINS_JOB_URL}]
  {code:java}
  ${STACK_TRACE}
  {code}
EOF
  )
  export COMMENT=$(echo "${COMMENT}" | jq -sR)

  # Add details of flaky failure as a new Jira comment
  ${SCRIPT_DIR}/add_comment.sh
done
