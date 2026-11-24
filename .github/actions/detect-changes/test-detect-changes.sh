#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
TEST_DIR=$(mktemp -d)
trap 'rm -rf "$TEST_DIR"' EXIT
export GITHUB_OUTPUT="$TEST_DIR/output"
TEST_COUNT=0

assert_build() {
  local expected=$1 description=$2
  : > "$GITHUB_OUTPUT"
  rm -f "$TEST_DIR/metadata-calls"
  if ! bash "$SCRIPT_DIR/detect-changes.sh" > "$TEST_DIR/log" 2>&1; then
    cat "$TEST_DIR/log" >&2
    echo "FAIL: $description (detector failed)" >&2
    exit 1
  fi
  if [[ $(cat "$GITHUB_OUTPUT") != "build-type=$expected" ]]; then
    cat "$TEST_DIR/log" >&2
    echo "FAIL: $description (expected $expected)" >&2
    exit 1
  fi
  TEST_COUNT=$((TEST_COUNT + 1))
  echo "PASS: $description"
}

commit() {
  git add .
  git -c commit.gpgsign=false commit -qm test
  export GITHUB_SHA
  GITHUB_SHA=$(git rev-parse HEAD)
}

reset_repo() {
  git reset --hard -q "$BASE_SHA"
  git clean -fdq
  export EVENT_NAME=pull_request
}

mkdir "$TEST_DIR/repo"
cd "$TEST_DIR/repo"
git init -q
git config user.email test@example.com
git config user.name Test
mkdir core documentation
echo 'class Source {}' > core/Source.java
echo Docs > documentation/topic.adoc
commit
export BASE_SHA EVENT_NAME=pull_request
BASE_SHA=$GITHUB_SHA
assert_build skip 'empty diff'

echo Readme > README.md
mkdir nested
echo Instructions > nested/AI-CODE.md
echo License > LICENSE.txt
echo Ignored > .gitignore
commit
assert_build skip 'accessory-only changes'

echo More >> documentation/topic.adoc
commit
assert_build docs-only 'documentation and accessory changes'

echo Docs > $'documentation/a space\nand newline.adoc'
commit
assert_build docs-only 'documentation filename with spaces and a newline'

echo 'class Source { int value; }' > core/Source.java
commit
assert_build full 'mixed code and documentation changes'
export EVENT_NAME=push
echo More >> README.md
commit
assert_build full 'push checks every commit, not just the last'

reset_repo
git mv core/Source.java documentation/Source.adoc
commit
assert_build full 'source renamed into documentation'

reset_repo
git mv core/Source.java Source.md
commit
assert_build full 'source renamed into an ignored Markdown path'

reset_repo
rm core/Source.java
commit
assert_build full 'source deletion'

ORIGINAL_BASE=$BASE_SHA
for BASE_SHA in '' 0000000000000000000000000000000000000000 ffffffffffffffffffffffffffffffffffffffff; do
  assert_build full "missing history defaults to full ($BASE_SHA)"
done
BASE_SHA=$ORIGINAL_BASE

# Exercise the trusted API path with actual JSON parsing and no network access.
mkdir "$TEST_DIR/bin"
cat > "$TEST_DIR/bin/gh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  'api repos/owner/repo/pulls/42')
    [[ "${MOCK_FAILURE:-}" != metadata ]] || exit 1
    if [[ -f "$FIXTURES/metadata-calls" ]]; then
      cat "$FIXTURES/after.json"
    else
      touch "$FIXTURES/metadata-calls"
      cat "$FIXTURES/before.json"
    fi
    ;;
  'api --paginate --slurp repos/owner/repo/pulls/42/files?per_page=100')
    [[ "${MOCK_FAILURE:-}" != files ]] || exit 1
    cat "$FIXTURES/pages.json"
    ;;
  *) echo "Unexpected gh invocation: $*" >&2; exit 1 ;;
esac
MOCK
chmod +x "$TEST_DIR/bin/gh"
export PATH="$TEST_DIR/bin:$PATH" FIXTURES="$TEST_DIR"
export EVENT_NAME=workflow_run SOURCE_EVENT=pull_request PR_NUMBER=42
export SOURCE_HEAD_SHA=head GITHUB_REPOSITORY=owner/repo MOCK_FAILURE=''

metadata() {
  jq -cn --arg head "${1:-head}" --arg base "${2:-base}" --argjson count "${3:-1}" \
    '{head: {sha: $head}, base: {sha: $base}, changed_files: $count}' > "$TEST_DIR/before.json"
  cp "$TEST_DIR/before.json" "$TEST_DIR/after.json"
}

metadata head base 2
echo '[[{"filename":"documentation/topic.adoc"}],[{"filename":"README.md"}]]' > "$TEST_DIR/pages.json"
assert_build docs-only 'paginated documentation and accessory files'

metadata
echo '[[{"filename":"README.md"}]]' > "$TEST_DIR/pages.json"
assert_build skip 'trusted accessory-only classification'
printf '%s\n' '[[{"filename":"documentation/a space\nand newline.adoc"}]]' > "$TEST_DIR/pages.json"
assert_build docs-only 'API filename with spaces and a newline'
echo '[[{"filename":"documentation/topic.adoc","previous_filename":"core/Source.java"}]]' > "$TEST_DIR/pages.json"
assert_build full 'API rename includes removed source'
echo skip > build-type.txt
echo '[[{"filename":"core/Source.java"}]]' > "$TEST_DIR/pages.json"
assert_build full 'PR artifact cannot waive code tests'

echo '[[{"filename":"README.md"}]]' > "$TEST_DIR/pages.json"
metadata old
assert_build full 'stale PR head'
metadata head base 3000
assert_build full 'API file limit'
metadata head base 2
assert_build full 'incomplete API file list'
metadata
jq '.head.sha = "new"' "$TEST_DIR/before.json" > "$TEST_DIR/after.json"
assert_build full 'PR head changes during pagination'
metadata
jq '.base.sha = "new"' "$TEST_DIR/before.json" > "$TEST_DIR/after.json"
assert_build full 'PR base changes during pagination'
metadata
PR_NUMBER='' assert_build full 'unidentified PR'
PR_NUMBER='42/invalid' assert_build full 'invalid PR number'
SOURCE_EVENT=push assert_build full 'unexpected source event'
MOCK_FAILURE=metadata assert_build full 'metadata request failure'
MOCK_FAILURE=files assert_build full 'file request failure'
echo 'invalid JSON' > "$TEST_DIR/before.json"
assert_build full 'invalid metadata JSON'
metadata
echo 'invalid JSON' > "$TEST_DIR/pages.json"
assert_build full 'invalid file list JSON'
echo '[[{"filename":null}]]' > "$TEST_DIR/pages.json"
assert_build full 'invalid filename metadata'
metadata head base 0
echo '[[]]' > "$TEST_DIR/pages.json"
assert_build skip 'empty trusted PR file list'

echo "All $TEST_COUNT change-detection checks passed"
