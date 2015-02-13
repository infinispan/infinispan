#!/bin/bash

while [[ $# > 1 ]]
do
key="$1"
case $key in
    -r|--remote)
    REMOTE="$2"
    shift
    ;;
    -pr|--pull-request)
    PULL_REQUEST="$2"
    shift
    ;;
    -db|--destination-branch)
    DESTINATION_BRANCH="$2"
    shift
    ;;
    --dry-run)
    DRY_RUN=--dry-run
    shift
    ;;
    --setup)
    SETUP=1
    ;;
    *)
    shift
    ;;
esac
shift
done

if [ "x$SETUP" != "x" ] && [ "x$REMOTE" != "x" ]; then

	echo "Setting up remote for Pull Request integration"
	git config --replace-all remote.$REMOTE.fetch "+refs/heads/*:refs/remotes/$REMOTE/*"
	git config --add remote.$REMOTE.fetch "+refs/pull/*:refs/remotes/$REMOTE/pr/*"
	echo "Fetching new refspecs"
	git fetch $REMOTE --prune
	echo "Done"
	exit 1
fi

if [ "x$REMOTE" = "x" ] || [ "x$PULL_REQUEST" = "x" ] || [ "x$DESTINATION_BRANCH" = "x" ]; then
    echo "Usage:"
    echo "./integrate_pr.sh -r <REMOTE_NAME> -pr <PULL_REQUEST_NUMBER> -db <DESTINATION_BRANCH>"
    echo "e.g. ./integrate_pr.sh -db master -pr 332 -r upstream"
    echo ""
    echo "Setting up remote for Pull Request integration:"
    echo "./integrate_pr.sh --setup -r <REMOTE_NAME>"
    echo "e.g. ./integrate_pr.sh --setup -r master"
    exit 1
fi

set -e

echo "Rebasing $REMOTE/pr/$PULL_REQUEST/head onto $REMOTE/$DESTINATION_BRANCH"
git rebase --onto $REMOTE/$DESTINATION_BRANCH $REMOTE/$DESTINATION_BRANCH $REMOTE/pr/$PULL_REQUEST/head
git checkout -B $DESTINATION_BRANCH 
echo "Pushing to REMOTE"
git push $DRY_RUN $REMOTE
echo "Removing merge branch: $REMOTE/pr/$PULL_REQUEST/merge"
# This may fail if pushing above commits resulted in fast forward
git push $DRY_RUN $REMOTE :$REMOTE/pr/$PULL_REQUEST/merge || true



