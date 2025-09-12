#!/usr/bin/env bash
set -euo pipefail

BASE_REF=$1
TARGET_REF=$2

# Get changed Java files from all modules (main + test)
FILES=$(git diff --name-only $BASE_REF...$TARGET_REF | grep -E '.*/src/(main|test)/java/.*\.java$' || true)

if [[ -z "$FILES" ]]; then
  echo "NO_CHANGES"
  exit 0
fi

# Convert file paths to FQCNs (strip module prefix + src path, replace / with .)
CLASSES=$(echo "$FILES" | sed -E 's#.*/src/(main|test)/java/##; s#/#.#g; s#.java$##' | sort -u | tr '\n' ',' | sed 's/,$//')

if [[ -z "$CLASSES" ]]; then
  echo "NO_CHANGES"
  exit 0
fi

echo "$CLASSES"
