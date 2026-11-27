#!/usr/bin/env bash
set -euo pipefail

# Shared exclusions for the Build, Test, and Native CLI workflows.
IGNORED_PATHS=('**.md' 'LICENSE*' '.gitignore' '.gitattributes' '.editorconfig' 'CODEOWNERS')
DOCS_PATHS=('documentation/**')

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

pr_metadata() {
  gh api "$1" | jq -ce '
    {head: .head.sha, base: .base.sha, count: .changed_files}
    | select((.head | type) == "string" and (.base | type) == "string"
        and (.count | type) == "number")
    | select(.count >= 0 and .count < 3000 and .count == (.count | floor))'
}

changed_files() {
  if [[ "$EVENT_NAME" == workflow_run ]]; then
    # Never run PR code or trust a PR-produced artifact to waive required tests.
    [[ "${SOURCE_EVENT:-}" == pull_request && "${PR_NUMBER:-}" =~ ^[0-9]+$ ]] || return 1
    local endpoint="repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER"
    local before after count
    before=$(pr_metadata "$endpoint") || return 1
    [[ $(jq -r '.head' <<< "$before") == "$SOURCE_HEAD_SHA" ]] || return 1
    count=$(jq -r '.count' <<< "$before") || return 1
    gh api --paginate --slurp "$endpoint/files?per_page=100" > "$WORK_DIR/pages.json" || return 1
    after=$(pr_metadata "$endpoint") || return 1
    # Detect updates during pagination, including base-branch changes.
    [[ "$before" == "$after" ]] || return 1
    jq -e --argjson count "$count" '
      add | length == $count and all(.[];
        (.filename | type) == "string"
        and ((.previous_filename // "") | type) == "string")
    ' "$WORK_DIR/pages.json" > /dev/null || return 1
    jq -j '.[][] | .filename, (.previous_filename // empty) | ., "\u0000"' \
      "$WORK_DIR/pages.json" > "$WORK_DIR/changed-files" || return 1
  else
    [[ -n "${BASE_SHA:-}" && "$BASE_SHA" != 0000000000000000000000000000000000000000 ]] || return 1
    # Include removed paths for renames; NUL separators preserve unusual filenames.
    if [[ "$EVENT_NAME" == pull_request ]]; then
      git diff --no-renames --name-only -z "$BASE_SHA...$GITHUB_SHA" > "$WORK_DIR/changed-files" || return 1
    else
      git diff --no-renames --name-only -z "$BASE_SHA" "$GITHUB_SHA" > "$WORK_DIR/changed-files" || return 1
    fi
  fi
}

BUILD_TYPE=full
if changed_files; then
  BUILD_TYPE=skip
  while IFS= read -r -d '' FILE; do
    IGNORED=false
    for PATTERN in "${IGNORED_PATHS[@]}"; do
      # Match the glob rather than the literal pattern string.
      # shellcheck disable=SC2053
      if [[ "$FILE" == $PATTERN ]]; then
        IGNORED=true
        break
      fi
    done
    [[ "$IGNORED" == true ]] && continue

    DOCS=false
    for PATTERN in "${DOCS_PATHS[@]}"; do
      # shellcheck disable=SC2053
      if [[ "$FILE" == $PATTERN ]]; then
        DOCS=true
        break
      fi
    done
    if [[ "$DOCS" == true ]]; then
      BUILD_TYPE=docs-only
    else
      BUILD_TYPE=full
      break
    fi
  done < "$WORK_DIR/changed-files"
else
  echo 'Cannot classify changes, defaulting to full'
fi

echo "Build type: $BUILD_TYPE"
echo "build-type=$BUILD_TYPE" >> "$GITHUB_OUTPUT"
