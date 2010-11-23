#!/usr/bin/python
import re
import time
import sys
from utils import *

def main():
  start_time = time.clock()
  disabled_test_files = []
  
  test_annotation_matcher = re.compile('^\s*@Test')
  disabled_matcher = re.compile('enabled\s*=\s*false')
  
  for test_file in GlobDirectoryWalker(get_search_path(sys.argv[0]), '*Test.java'):
    tf = open(test_file)
    try:
      for line in tf:
        if test_annotation_matcher.search(line) and disabled_matcher.search(line):
          disabled_test_files.append(test_file)
          break
    finally:
      tf.close()
      
  print "Files containing disabled tests: \n"
  unique_tests=to_set(disabled_test_files)
  i = 1
  for f in unique_tests:
    zeropad=""
    if i < 10 and len(unique_tests) > 9:
      zeropad = " "
    print "%s%s. %s" % (zeropad, str(i), strip_leading_dots(f))
    i += 1

  print "\n      (finished in " +  str(time.clock() - start_time) + " seconds)"
  
if __name__ == '__main__':
  main()
  
