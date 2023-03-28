#!/bin/sh

DIRNAME=$(dirname "$0")

# Setup ISPN_HOME
ISPN_HOME=$(cd "$DIRNAME/.." > /dev/null || exit; pwd)

USER_NAME=$(whoami)
SERVERS=""
ADDITIONAL_ARGS=()
IDX=-1

for arg in "$@"
do
  case "$arg" in
  --user=*)
    USER_NAME="${arg#*=}"
    ;;
  --server=*)
    IDX=$((IDX+1))
    ADDITIONAL_ARGS[$IDX]=""
    SERVERS="$SERVERS ${arg#*=}"
    ;;
  *)
    ADDITIONAL_ARGS[$IDX]="${ADDITIONAL_ARGS[$IDX]} $arg"
    ;;
  esac
done

if [ "x$SERVERS" = "x" ]; then
  echo "No servers arguments specified" >&2
  exit 1
fi

IDX=0
for server in $SERVERS; do
  SCRIPT="$ISPN_HOME/bin/server.sh"
  if [ "x${ADDITIONAL_ARGS[$IDX]}" != "x" ]; then
    SCRIPT="$SCRIPT ${ADDITIONAL_ARGS[$IDX]}"
  fi
  # We want this expanded on the client side.
  ssh -f -n -q "$USER_NAME@$server" "$SCRIPT"
  IDX=$((IDX+1))
done
