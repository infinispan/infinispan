#!/usr/bin/python

import re
import sys
from pythonTools import *


command_file_name = re.compile('(commands/[a-zA-Z0-9/]*Command.java)')

def trimName(nm):
  res = command_file_name.search(nm)
  if res:
    return res.group(1)
  else:
    return nm

def get_next(ids_used):
  # Cannot assume a command ID greater than the size of 1 byte!
  for i in range(1, 128):
    if i not in ids_used:
      return i
  return -1


command_line_regexp = re.compile('COMMAND_ID\s*=\s*([0-9]+)\s*;')


command_ids = {}

for testFile in GlobDirectoryWalker(getSearchPath(sys.argv[0]) + 'core/src/main/java/org/infinispan/commands', '*Command.java'):
  tf = open(testFile)
  try:
    for line in tf:
      mo = command_line_regexp.search(line)
      if mo:
        command_ids[int(mo.group(1))] = trimName(testFile)
  finally:
    tf.close()

print 'Scanned %s Command source files.  IDs are (in order):' % len(command_ids)

sortedKeys = command_ids.keys()
sortedKeys.sort()

i=1
for k in sortedKeys:
  print '   %s) Class [%s] has COMMAND_ID [%s]' % (i, command_ids[k], k)
  i += 1

print "\n"
print "Next available ID is %s" % get_next(sortedKeys)
