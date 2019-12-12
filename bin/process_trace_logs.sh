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

if [[ -z "$DELETE_COMMAND" ]]; then
  DELETE_COMMAND="rm"
fi

for FAILURES_LOG in $(find "$ROOT_DIR" -name 'test-failures*.log'); do
  LOG_DIR=$(dirname "$FAILURES_LOG")

  # Find the failed tests in this failures log and move them
  FAILED_TESTS=$(sed -n -r 's/.*TestSuiteProgress.*[^[:alnum:]]([[:alnum:]]+Test)\..*/\1/ p' "$FAILURES_LOG" | sort -u)
  for TEST in $FAILED_TESTS; do
    if test -n "$(find ${LOG_DIR} -maxdepth 1 -name "*${TEST}*" -print -quit)"; then
      for LOG in ${LOG_DIR}/*${TEST}*; do
        echo $LOG
        BASENAME=$(basename "$LOG")
        mv "$LOG" "${DEST_PREFIX}${BASENAME}"
      done
    else
      echo "Log missing: ${LOG_DIR}/*${TEST}*"
    fi
  done

  # Remove the tests that didn't fail
  find ${LOG_DIR} -name '*Test*.log.gz' -exec $DELETE_COMMAND {} \;

  # Move any remaining logs
  for LOG in $(find ${LOG_DIR} -maxdepth 1 -name '*.log.gz'); do
    BASENAME=$(basename "$LOG")
    echo "${DEST_PREFIX}${BASENAME}"
    mv "$LOG" "${DEST_PREFIX}${BASENAME}"
  done
done
