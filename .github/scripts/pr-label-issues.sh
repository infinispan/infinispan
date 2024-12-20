#!/bin/bash -e

PR="$1"
REPO="$2"
REF="$3"

echo "**Branch:** [$REF](https://github.com/$REPO/tree/$REF)"
echo "**PR:** [$PR](https://github.com/$REPO/pull/$PR)"

if [[ $REF == main ]]; then
  LAST_RELEASE="$(gh api /repos/$REPO/branches --paginate --jq .[].name | grep -E '^[0-9]+\.[0-9]+\.x' | sort -n -r | head -n 1)"
  LAST_MAJOR=$(echo "$LAST_RELEASE" | cut -d '.' -f 1)
  LAST_MINOR=$(echo "$LAST_RELEASE" | cut -d '.' -f 2)

  NEXT_MAJOR=$LAST_MAJOR
  NEXT_MINOR="$(($LAST_MINOR + 1))"
  LABEL="release/$NEXT_MAJOR.$NEXT_MINOR.0"
  EXTRA_EDIT=""
elif [[ $REF =~ ^[0-9]+\.[0-9]+\.x$ ]]; then
  MAJOR_MINOR="$(echo $REF | cut -d . -f 1,2)"
  LAST_MICRO="$(gh api /repos/$REPO/tags --jq .[].name | sort -V -r | grep $MAJOR_MINOR | head -n 1 | cut -d . -f 3)"
  NEXT_MICRO="$(($LAST_MICRO + 1))"
  LABEL="release/$MAJOR_MINOR.$NEXT_MICRO"
  EXTRA_EDIT="--remove-label backport/$MAJOR_MINOR"
fi

echo "**Label:** [$LABEL](https://github.com/$REPO/labels/$LABEL)"

gh api "/repos/$REPO/labels/$LABEL" --silent 2>/dev/null || gh label create -R "$REPO" "$LABEL" -c "0E8A16"

echo "**Updating issues:**"

ISSUES=$(.github/scripts/pr-find-issues.sh "$PR" "$REPO")
for ISSUE in $ISSUES; do
  echo "* [$ISSUE](https://github.com/$REPO/issues/$ISSUE)"
  gh issue edit "$ISSUE" -R "$REPO" --add-label "$LABEL" $EXTRA_EDIT
done