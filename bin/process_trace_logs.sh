#!/bin/bash

set -e

SCRIPT_NAME=$(basename "$0")
SCRIPT_DIR=$(dirname "$0")

if [[ "$1" == "-h" || "$1" == "" ]]; then
  echo "Usage: $SCRIPT_NAME LOG_DIR [DEST_DIR/][FILE_PREFIX]"
  echo "
  Move the logs of failed tests in LOG_DIR or its subdirectories to DEST_DIR,
  optionally adding a common prefix.
  Assumes each test has a separate log file named \`*FullNameTest*.log.gz\`.
  A test is considered failed if its name appears in a file \`test-failures-*.log\`
  in the same directory as the log file and matches the regular expression
  \`.*TestSuiteProgress.*FullNameTest.*\`."
  exit 1
fi

ROOT_DIR="$1"
DEST_PREFIX="$2"

for FAILURES_LOG in $(find "$ROOT_DIR" -name 'test-failures*.log'); do
  LOG_DIR=$(dirname "$FAILURES_LOG")

  # Find the failed tests in this failures log and move them
  FAILED_TESTS=$(sed -n -r 's/.*TestSuiteProgress.*[^[:alnum:]]([[:alnum:]]+Test).*/\1/ p' "$FAILURES_LOG" | sort -u)
  for TEST in $FAILED_TESTS; do
    for LOG in ${LOG_DIR}/*${TEST}*; do
      BASENAME=$(basename "$LOG")
      echo "${DEST_PREFIX}${BASENAME}"
      mv "$LOG" "${DEST_PREFIX}${BASENAME}"
    done
  done

  # Remove the tests that didn't fail
  rm -f ${LOG_DIR}/*Test*.log.gz

  # Move any remaining logs
  for LOG in ${LOG_DIR}/*.log.gz; do
    BASENAME=$(basename "$LOG")
    echo "${DEST_PREFIX}${BASENAME}"
    mv "$LOG" "${DEST_PREFIX}${BASENAME}"
  done
done