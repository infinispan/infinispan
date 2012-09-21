#!/usr/bin/python

import re
import sys
from utils import *

command_file_name = re.compile('([a-zA-Z0-9/]*Command.java)')

def trim_name(nm):
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
warnings = []
for test_file in GlobDirectoryWalker(get_search_path(sys.argv[0]) + 'core/src/main/java/', '*Command.java'):
  tf = open(test_file)
  try:
    for line in tf:
      mo = command_line_regexp.search(line)
      if mo:
        id = int(mo.group(1))
        trimmed_name = trim_name(test_file)
        if id in command_ids:
          warnings.append("Saw duplicate COMMAND_IDs in files [%s] and [%s]" % (trimmed_name, command_ids[id])) 
        command_ids[id] = trimmed_name
  finally:
    tf.close()

print 'Scanned %s Command source files.  IDs are (in order):' % len(command_ids)

sorted_keys = command_ids.keys()
sorted_keys.sort()

i=1
prev_id = 0
for k in sorted_keys:
  prev_id += 1
  while k > prev_id:
    print '  ---'
    prev_id += 1

  zeropad = ""
  if (i < 10 and len(sorted_keys) > 9):
    zeropad = " "
  print '   %s%s) Class [%s%s%s] has COMMAND_ID [%s%s%s]' % (zeropad, i, Colors.green(), command_ids[k], Colors.end_color(), Colors.yellow(), k, Colors.end_color())
  i += 1

print "\n"
if len(warnings) > 0:
  print "WARNINGS:"
  for w in warnings:
    print "  *** %s" % w
  print "\n"

print "Next available ID is %s%s%s" % (Colors.cyan(), get_next(sorted_keys), Colors.end_color())
