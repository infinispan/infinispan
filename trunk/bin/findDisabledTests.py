#!/usr/bin/python
import re
import time
import sys
from pythonTools import *

def main():
  startTime = time.clock()
  disabledTestFiles = []
  
  testAnnotationMatcher = re.compile('^\s*@Test')
  disabledMatcher = re.compile('enabled\s*=\s*false')
  
  for testFile in GlobDirectoryWalker(getSearchPath(sys.argv[0]), '*Test.java'):
    tf = open(testFile)
    try:
      for line in tf:
        if testAnnotationMatcher.search(line) and disabledMatcher.search(line):
          disabledTestFiles.append(testFile)
          break
    finally:
      tf.close()
      
  print "Files containing disabled tests: \n"
  uniqueTests=toSet(disabledTestFiles)
  i = 1
  for f in uniqueTests:
    zeropad=""
    if i < 10 and len(uniqueTests) > 9:
      zeropad = " "
    print "%s%s. %s" % (zeropad, str(i), stripLeadingDots(f))
    i += 1

  print "\n      (finished in " +  str(time.clock() - startTime) + " seconds)"
  
if __name__ == '__main__':
  main()
  
