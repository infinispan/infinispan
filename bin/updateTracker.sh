#!/bin/bash

OFFLINE_TRACKER_ID="UA-8601422-4"
TRACKER_ID="UA-8601422-2"

echo Executing in current directory

for f in `find . -name "*.html"` ; do
 sed -e "s/${OFFLINE_TRACKER_ID}/${TRACKER_ID}/g" ${f} > ${f}.tmp
 mv ${f}.tmp ${f}
done

echo Done

