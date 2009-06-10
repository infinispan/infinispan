#!/usr/bin/python

from __future__ import with_statement

import re
import subprocess
import os

VIEW_TO_USE = '3'
INPUT_FILE = "infinispan.log"
OUTPUT_FILE = "infinispan0.log"
addresses = {}
new_addresses = {}
def find(expr):
  with open(INPUT_FILE) as f:
    for l in f:
      if expr.match(l):
        handle(l, expr)
        break

def handle(l, expr):
 # print l
  m = expr.match(l)
  members = m.group(1).strip()
  i = 1
  for m in members.split(','):
    addresses[m.strip()] = "CACHE%s" % i
    new_addresses["CACHE%s" % i] = m.strip()
    i += 1

def main():

  print """

	  INFINISPAN Log file fixer.  Makes log files more readable by replacing ugly JGroups addresses to more friendly CACHE1, CACHE2, etc addresses.

	  Usage:

	    $ bin/fixlogs.py

	  TODO: be able to specify which view to select as the correct one, and to specify input and output files.

  """ 
  expr = re.compile('.*Received new cluster view.*\|%s. \[(.*)\].*' % VIEW_TO_USE)
  find(expr)

  with open(INPUT_FILE) as f_in:
    with open(OUTPUT_FILE, 'w+') as f_out:
      for l in f_in:
        for c in addresses.keys():
          l = l.replace(c, addresses[c])
        f_out.write(l)

  print "Processed %s and generated %s.  The following replacements were made: " % (INPUT_FILE, OUTPUT_FILE)	
  sorted_keys = new_addresses.keys()
  sorted_keys.sort()
  for a in sorted_keys:
    print "  %s --> %s" % (new_addresses[a], a)

if __name__ == "__main__":
	main()
