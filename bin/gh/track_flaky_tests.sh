#!/bin/bash -x
set -e
# A script to track flaky tests in Github
# Requires xmlstarlet and jq to be installed
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

requiredEnv GH_JOB_URL FLAKY_TEST_GLOB TARGET_BRANCH GITHUB_REPOSITORY

shopt -s nullglob globstar
TESTS=(${FLAKY_TEST_GLOB})
for TEST_FILE in "${TESTS[@]}"; do
  # Sometimes flaky plugin writes two root elements in the same file
  # producing invalid xml. We need to split them into separate files
  cat ${TEST_FILE} | grep -v '^<?xml ' | csplit -z -f csplit-flaky-xml- - '/^<testsuite /' '{*}'
  for TEST in csplit-flaky-xml-*; do
    API_LIMIT_TIME=0
    TEST_CLASS_NAMES=$(xmlstarlet sel -t --value-of '/testsuite/testcase/@classname'  ${TEST})
    declare -i i
    for TEST_CLASS in $TEST_CLASS_NAMES; do
      # just get the first one for now
      i=1
      TEST_NAME=$(xmlstarlet sel --template --value-of '/testsuite/testcase['$i']/@name' ${TEST})
      # Removing (Flaky Test) text
      TEST_NAME=${TEST_NAME% (Flaky Test)}
      # Some tests have arguments with backslash, ie testReplace\[NO_TX, P_TX\]. Removing
      TEST_NAME_NO_PARAMS=${TEST_NAME%%\\\[*}
      # Some tests have it without backslash or have fail counter, ie testContainsAll[1]. Removing
      TEST_NAME_NO_PARAMS=${TEST_NAME_NO_PARAMS%%[*}
      # Some tests end with \(. Removing
      TEST_NAME_NO_PARAMS=${TEST_NAME_NO_PARAMS%%\\\(*}
      # Some tests end with (. Removing
      TEST_NAME_NO_PARAMS=${TEST_NAME_NO_PARAMS%%(*}
      STACK_TRACE=$(xmlstarlet sel --template --value-of '/testsuite/testcase/failure['$i']' ${TEST})

      # Create Issue for Test Class+TestName
      SUMMARY="Flaky test: ${TEST_CLASS}#${TEST_NAME_NO_PARAMS}"
      echo "${SUMMARY}"

      # Search issues for existing github issue
      # Wait some time for subsequent request to respect API rate limit
      sleep $API_LIMIT_TIME
      ISSUES="$(gh search issues --json number --repo $GITHUB_REPOSITORY -- "$(printf '%s' "$SUMMARY")" in:title || true)"
      API_LIMIT_TIME=120
      if [[ "${ISSUES}" == "" ]]; then
        echo Error with gh search. Maybe rate limits reached? Wait 120 sec and retry...
        gh api rate_limit
        sleep $API_LIMIT_TIME
        ISSUES="$(gh search issues --json number --repo $GITHUB_REPOSITORY -- "$(printf '%s' "$SUMMARY")" in:title || true)"
        continue
      fi
      TOTAL_ISSUES=$(echo "${ISSUES}" | jq length)
      if [ ${TOTAL_ISSUES} -gt 1 ]; then
        echo "Multiple issues for same flaky test: $(printf '%s' "$SUMMARY")"
        exit 1
      fi

        BODY=$(printf "### Target Branch: %s\n### Github Job:%s\n### Test method:%s\n%s" "${TARGET_BRANCH}" "${GH_JOB_URL}" "$TEST_NAME" "${STACK_TRACE}")
      if [ ${TOTAL_ISSUES} == 0 ]; then
        echo "Existing issue not found, creating a new one"
        gh issue create --title "${SUMMARY}" --body "${BODY}" --label "kind/flaky test" --repo $GITHUB_REPOSITORY
      else
        export ISSUE_KEY=$(echo "${ISSUES}" | jq  '.[0].number')
        # Re-open the issue if it was previously resolved
        if [ "$(gh issue view ${ISSUE_KEY} --json state | jq .state)" == '"CLOSED"' ]; then
          gh issue reopen ${ISSUE_KEY}  --repo $GITHUB_REPOSITORY
        fi
        gh issue comment ${ISSUE_KEY} --body "${BODY}" --repo $GITHUB_REPOSITORY
      fi
    done
  done
  rm -f split-flaky-xml-*
done
