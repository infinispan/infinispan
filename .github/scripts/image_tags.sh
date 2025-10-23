#!/usr/bin/env bash
set -e

if [[ "$RUNNER_DEBUG" == "1" ]]; then
  set -x
fi

function requiredEnv() {
  for ENV in $@; do
      if [ -z "${!ENV}" ]; then
        echo "${ENV} variable must be set"
        exit 1
      fi
  done
}

requiredEnv VERSION TYPE

MAJOR_MINOR=$(echo "$VERSION"|cut -d . -f 1,2)

TAGS="${MAJOR_MINOR} ${VERSION}"
if [ "${TYPE}" != "prerelease" ]; then
  TAGS+=" latest"
fi

FQ_TAGS=""
for TAG in ${TAGS}; do
  FQ_TAGS+="${REPO}infinispan/server:${TAG} "
done
echo "${FQ_TAGS::-1}"
