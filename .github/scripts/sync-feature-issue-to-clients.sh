#!/bin/bash -e

ISSUE="$1"
REPO="$2"

# Space-separated list of client repositories the issue is cloned to. Can be overridden via the CLIENT_TARGETS env var.
CLIENT_TARGETS="${CLIENT_TARGETS:-infinispan/js-client infinispan/go-client infinispan/dotnet-client}"

echo "**Issue:** [$ISSUE](https://github.com/$REPO/issues/$ISSUE)"

OWNER=${REPO%%/*}
NAME=${REPO##*/}

ISSUE_JSON=$(gh api "/repos/$REPO/issues/$ISSUE")

if [[ "$(echo "$ISSUE_JSON" | jq 'has("pull_request")')" == "true" ]]; then
  echo "This is a pull request, not an issue. Nothing to do."
  exit 0
fi

HAS_LABEL=$(echo "$ISSUE_JSON" | jq '[.labels[]?.name] | index("area/client/hotrod")')
if [[ -z "$HAS_LABEL" || "$HAS_LABEL" == "null" ]]; then
  echo 'Issue does not have the area/client/hotrod label. Nothing to do.'
  exit 0
fi

# The issue type is only available through GraphQL, so query it there.
ISSUE_TYPE=$(gh api graphql \
  -f query='query($owner: String!, $name: String!, $number: Int!) { repository(owner: $owner, name: $name) { issue(number: $number) { issueType { name } } } }' \
  -F owner="$OWNER" -F name="$NAME" -F number="$ISSUE" --jq '.data.repository.issue.issueType.name // empty')

if [[ "$ISSUE_TYPE" != "Feature" ]]; then
  echo "Issue type is '${ISSUE_TYPE:-unknown}', not 'Feature'. Nothing to do."
  exit 0
fi

TITLE=$(echo "$ISSUE_JSON" | jq -r '.title')
BODY=$(echo "$ISSUE_JSON" | jq -r '.body // empty')
URL=$(echo "$ISSUE_JSON" | jq -r '.html_url')

# Prefix the title so synced issues are recognizable and can be deduplicated.
PREFIX="[Synced from $REPO#$ISSUE] "
MAX_LEN=$((256 - ${#PREFIX})) # GitHub limits issue titles to 256 characters
if [[ "${#TITLE}" -gt "$MAX_LEN" ]]; then
  TITLE="${TITLE:0:$((MAX_LEN - 3))}..."
fi
NEW_TITLE="$PREFIX$TITLE"

CLONED_BODY=$(cat <<EOF
${BODY}

---
This feature request was automatically synced from [${REPO}#${ISSUE}](${URL}). Please track the implementation for this client in this repository.
EOF
)

for TARGET in $CLIENT_TARGETS; do
  echo "**Syncing to:** $TARGET"
  # Match open and closed issues so a clone dismissed as not applicable is never recreated.
  EXISTING=$(gh api --paginate "/repos/$TARGET/issues?state=all&per_page=100" \
    | jq -s --arg t "$NEW_TITLE" '[.[][] | select(.title == $t)] | length')
  if [[ "$EXISTING" != "0" ]]; then
    echo "* Already synced to $TARGET, skipping."
    continue
  fi
  NEW_ISSUE=$(gh api -X POST "/repos/$TARGET/issues" --field title="$NEW_TITLE" --field body="$CLONED_BODY")
  NEW_URL=$(echo "$NEW_ISSUE" | jq -r '.html_url')
  echo "* Created: $NEW_URL"
done
