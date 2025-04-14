#!/bin/bash
set -e
# A script to track flaky tests in Github
# Requires xmlstarlet and jq to be installed
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv GH_JOB_URL FLAKY_TEST_GLOB TARGET_BRANCH GITHUB_REPOSITORY

shopt -s nullglob globstar
TESTS=(${FLAKY_TEST_GLOB})
API_LIMIT_TIME=0
for TEST in "${TESTS[@]}"; do
  TEST_CLASS_NAMES=$(xmlstarlet sel -t --value-of '/testsuite/testcase/@classname'  ${TEST})
  declare -i i
  for TEST_CLASS in $TEST_CLASS_NAMES; do
    # just get the first one for now
    i=1
    TEST_NAME=$(xmlstarlet sel --template --value-of '/testsuite/testcase['$i']/@name' ${TEST})
    # Removing (Flaky Test) text
    TEST_NAME=${TEST_NAME% (Flaky Test)}
    # Some tests have arguments with backslash, ie testReplace\[NO_TX, P_TX\]. Removing
    TEST_NAME=${TEST_NAME%%\[*}
    # Some tests have it without backslash or have fail counter, ie testContainsAll[1]. Removing
    TEST_NAME=${TEST_NAME%%[*}
    # Some tests end with \(. Removing
    TEST_NAME_NO_PARAMS=${TEST_NAME%%\(*}
    STACK_TRACE=$(xmlstarlet sel --template --value-of '/testsuite/testcase/failure['$i']' ${TEST})

    # Create Issue for Test Class+TestName
    SUMMARY="Flaky test: ${TEST_CLASS}#${TEST_NAME_NO_PARAMS}"
    echo ${SUMMARY}

    # Search issues for existing github issue
    # Wait some time for subsequent request to respect API rate limit
    sleep $API_LIMIT_TIME
    ISSUES="$(gh search issues \"${SUMMARY}\" in:title --json number --repo $GITHUB_REPOSITORY || true)"
    API_LIMIT_TIME=120
    if [[ "${ISSUES}" == "" ]]; then
      echo Error with gh search. Maybe rate limits reached? Wait 120 sec and retry...
      gh api rate_limit
      sleep $API_LIMIT_TIME
      ISSUES="$(gh search issues \"${SUMMARY}\" in:title --json number --repo $GITHUB_REPOSITORY|| true)"
      continue
    fi
    TOTAL_ISSUES=$(echo "${ISSUES}" | jq length)
    if [ ${TOTAL_ISSUES} -gt 1 ]; then
      echo "Multiple issues for same flaky test: ${SUMMARY}"
      exit 1
    fi

      BODY=$(printf "### Target Branch: %s\n### Github Job:%s\n%s" "${TARGET_BRANCH}" "${GH_JOB_URL}" "${STACK_TRACE}")
    if [ ${TOTAL_ISSUES} == 0 ]; then
      echo "Existing issue not found, creating a new one"
      gh issue create --title "${SUMMARY}" --body "${BODY}" --label "kind/flaky test"
    else
      export ISSUE_KEY=$(echo "${ISSUES}" | jq  '.[0].number')
      # Re-open the issue if it was previously resolved
      if [ "$(gh issue view ${ISSUE_KEY} --json state | jq .state)" == '"CLOSED"' ]; then
        gh issue edit {ISSUE_KEY} --remove-label Stale || true
        gh issue reopen ${ISSUE_KEY}
      fi
      gh issue comment ${ISSUE_KEY} --body "${BODY}"
    fi
  done
done
