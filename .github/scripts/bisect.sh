#!/bin/bash
# 
# Usage: ./bisect-java-test.sh <good_commit_sha> <maven_module_name> <test_class_or_method>
#
# Arguments:
#   $1 (good_commit_sha): The SHA of a commit where the test is known to PASS.
#   $2 (maven_module_name): The name of the Maven module containing the test (e.g., 'core-service').
#   $3 (test_class_or_method): The fully qualified test class name, optionally with a method name 
#                             (e.g., 'com.example.MyTest#testFailingFeature').
#
# The script assumes:
# - The current working directory is the root of your Git repository.
# - The current HEAD is the first known BAD commit (where the test fails).
# - 'mvn' is available in your PATH.

# Stop on errors, but allow the test command to fail gracefully inside the function.
set -e

GOOD_SHA=$1
MODULE_NAME=$2
TEST_NAME=$3
BAD_SHA=$4

# Input validation
if [ -z "$GOOD_SHA" ] || [ -z "$MODULE_NAME" ] || [ -z "$TEST_NAME" ]; then
    echo "Error: Missing arguments."
    echo "Usage: $0 <good_commit_sha> <maven_module_name> <test_class_or_method>"
    echo "Example: $0 a1b2c3d4 project-core com.example.MyTestClass#testFeature"
    exit 1
fi

# Ensure we are in a Git repository
if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    echo "Error: Must be run inside a Git repository."
    exit 1
fi

if [ "$GOOD_SHA" == "." ]; then
    CURRENT_COMMIT=$(git rev-parse HEAD)
    echo "--- Testing commit: $CURRENT_COMMIT ---"
    
    # Temporarily disable 'set -e' so a failing test doesn't exit the script immediately.
    set +e
    
    # Build the entire project first, no need to log everything
    mvn -q -B clean install -DskipTests -Dcheckstyle.skip -T 1C
    
    # Run the test
    mvn verify -B -pl "$MODULE_NAME" -Dtest="$TEST_NAME" -Dit.test="$TEST_NAME"

    MVN_EXIT_CODE=$?
    
    set -e # Re-enable 'set -e'
    ZIP_NAME="$HOME/logs-$CURRENT_COMMIT.zip"
    zip -9rp "$ZIP_NAME" "$MODULE_NAME"/target/failsafe-reports "$MODULE_NAME"/target/surefire-reports "$MODULE_NAME"/target/*.log
    echo "Created logs archive: $ZIP_NAME"

    # Map the Maven exit code to the Git Bisect exit code:
    if [ "$MVN_EXIT_CODE" -eq 0 ]; then
        echo "--- Test PASSED (mvn exit 0). Bisect marking as GOOD. ---"
        mv "$ZIP_NAME" "$HOME/logs-good-$CURRENT_COMMIT.zip"
        ls -al "$HOME"
        exit 0
    else
        echo "--- Test FAILED (mvn exit $MVN_EXIT_CODE). Bisect marking as BAD. ---"
        mv "$ZIP_NAME" "$HOME/logs-bad-$CURRENT_COMMIT.zip"
        ls -al "$HOME"
		    exit 1
    fi
fi

# Check if the known good commit exists
if ! git cat-file -t "$GOOD_SHA" > /dev/null 2>&1; then
    echo "Error: The provided good commit SHA '$GOOD_SHA' is not a valid git object."
    exit 1
fi

if [ "$BAD_SHA" -eq "" ]; then
    BAD_SHA=$(git rev-parse HEAD)
fi

echo "--- Starting Git Bisect Automation ---"
echo "Known BAD commit: $BAD_SHA"
echo "Known GOOD commit: $GOOD_SHA"
echo "Target Module: $MODULE_NAME"
echo "Target Test: $TEST_NAME"
echo "--------------------------------------"

# 1. Start the bisect with the known good and bad commits.
if ! git bisect start "$BAD_SHA" "$GOOD_SHA"
then
    echo "Error: Failed to start git bisect. Check the good/bad commit range."
    # Resetting bisect state just in case
    git bisect reset
    exit 1
fi

# Run the test function until the bad commit is found.
git bisect run "$0" . "$MODULE_NAME" "$TEST_NAME"

# Cleanup and report the result.
GIT_BISECT_EXIT_CODE=$?
git bisect reset

if [ "$GIT_BISECT_EXIT_CODE" -eq 0 ]; then
    echo "--- SUCCESS: The first bad commit has been identified above. ---"
else
    echo "--- FAILURE: Git Bisect did not complete successfully (exit code $GIT_BISECT_EXIT_CODE). Check previous output for errors. ---"
fi

exit "$GIT_BISECT_EXIT_CODE"

