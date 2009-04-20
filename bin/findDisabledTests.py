#!/usr/bin/python
from __future__ import with_statement
import os
import fnmatch
import re
import time
import sys

def assertValidPython():
    if (sys.version < 2.5):
        raise Error("Incompatible version of Python.  Need at least Python 2.5.0")


assertValidPython()


class GlobDirectoryWalker:
    # a forward iterator that traverses a directory tree

    def __init__(self, directory, pattern="*"):
        self.stack = [directory]
        self.pattern = pattern
        self.files = []
        self.index = 0

    def __getitem__(self, index):
        while 1:
            try:
                file = self.files[self.index]
                self.index = self.index + 1
            except IndexError:
                # pop next directory from stack
                self.directory = self.stack.pop()
                self.files = os.listdir(self.directory)
                self.index = 0
            else:
                # got a filename
                fullname = os.path.join(self.directory, file)
                if os.path.isdir(fullname) and not os.path.islink(fullname):
                    self.stack.append(fullname)
                if fnmatch.fnmatch(file, self.pattern):
                    return fullname

def getSearchPath(executable):
    inBinDir = re.compile('^.*/?bin/.*.py')
    if inBinDir.search(executable):
        return "./"
    else:
        return "../"

def stripLeadingDots(filename):
    return filename.strip('/. ')

## Walk through all files that end with Test.java
startTime = time.clock()
disabledTestFiles = []

testAnnotationMatcher = re.compile('^\s*@Test')
disabledMatcher = re.compile('enabled\s*=\s*false')

for testFile in GlobDirectoryWalker(getSearchPath(sys.argv[0]), '*Test.java'):
    with open(testFile) as tf:
        for line in tf:
            if testAnnotationMatcher.search(line):
                if disabledMatcher.search(line):
                    disabledTestFiles.append(testFile)
                break

print "Files containing disabled tests: \n"
uniqueTests=set(disabledTestFiles)
i=1
for f in uniqueTests:
    print str(i) + ". " + stripLeadingDots(f)
    i=i+1

print "\n      (finished in " +  str(time.clock() - startTime) + " seconds)"
