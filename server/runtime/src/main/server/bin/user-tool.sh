#!/bin/sh

DIRNAME=`dirname "$0"`

>&2 echo "This tool is deprecated and will be removed in the future. Please use \"cli.sh user create\" instead"

if [ "$#" -eq 0 ]; then
  . "$DIRNAME/cli.sh" user create --help
else
  . "$DIRNAME/cli.sh" user create "$@"
fi
