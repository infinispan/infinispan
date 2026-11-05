#!/bin/bash
# Shared normalization for flaky test names.
#
# Used by both .github/scripts/track_flaky_tests.sh (to build the issue title)
# and .github/actions/comment-pr-flaky-test (to build the lookup key), so the
# two always agree on the canonical "<Class>#<method>" form.
#
# Strips the " (Flaky Test)" suffix and the TestNG parameter list / fail
# counter. Surefire sometimes escapes the bracket as "\[" (e.g.
# testReplace\[NO_TX, P_TX\]); the escaping backslash sits before the '[', so
# we strip the escaped forms first and drop any leftover trailing backslash,
# otherwise the title ends with a stray '\' (see issue #18439).
normalizeFlakyTestName() {
  local name="$1"
  name=${name% (Flaky Test)}
  name=${name%%\\[*}   # escaped:  \[...
  name=${name%%[*}     # plain:    [...  (and fail counter [1])
  name=${name%%\\(*}   # escaped:  \(...
  name=${name%%(*}     # plain:    (...
  name=${name%\\}      # safety net: drop any trailing backslash
  printf '%s' "$name"
}
