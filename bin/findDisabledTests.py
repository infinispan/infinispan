#!/usr/bin/python
import re
import time
import sys
from pythonTools import *

## Walk through all files that end with Test.java
startTime = time.clock()
disabledTestFiles = []

testAnnotationMatcher = re.compile('^\s*@Test')
disabledMatcher = re.compile('enabled\s*=\s*false')

for testFile in GlobDirectoryWalker(getSearchPath(sys.argv[0]), '*Test.java'):
    tf = open(testFile)
    try:
        for line in tf:
            if testAnnotationMatcher.search(line):
                if disabledMatcher.search(line):
                    disabledTestFiles.append(testFile)
                break
    finally:
        tf.close()

print "Files containing disabled tests: \n"
uniqueTests=toSet(disabledTestFiles)
i=1
for f in uniqueTests:
    print str(i) + ". " + stripLeadingDots(f)
    i=i+1

print "\n      (finished in " +  str(time.clock() - startTime) + " seconds)"
