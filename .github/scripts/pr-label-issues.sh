#!/bin/bash -e

PR="$1"
REPO="$2"
REF="$3"

echo "**Branch:** [$REF](https://github.com/$REPO/tree/$REF)"
echo "**PR:** [$PR](https://github.com/$REPO/pull/$PR)"

LABEL=release/$(mvn -q help:evaluate -Dexpression=project.version -DforceStdout | awk -F- '{ print $1 }')

echo "**Label:** [$LABEL](https://github.com/$REPO/labels/$LABEL)"

gh api "/repos/$REPO/labels/$LABEL" --silent 2>/dev/null || gh label create -R "$REPO" "$LABEL" -c "0E8A16"

echo "**Updating issues:**"

ISSUES=$(.github/scripts/pr-find-issues.sh "$PR" "$REPO")
for ISSUE in $ISSUES; do
  echo "* [$ISSUE](https://github.com/$REPO/issues/$ISSUE)"
  gh issue edit "$ISSUE" -R "$REPO" --add-label "$LABEL" $EXTRA_EDIT
done
