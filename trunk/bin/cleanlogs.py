#!/usr/bin/python
from __future__ import with_statement

import re
import subprocess
import os
import sys

VIEW_TO_USE = '3'
INPUT_FILE = "infinispan.log"
OUTPUT_FILE = "infinispan0.log"
addresses = {}
new_addresses = {}

def find(filename, expr):
  with open(filename) as f:
    for l in f:
      if expr.match(l):
        handle(l, expr)
        break
        
def handle(l, expr):
  """Handles a given line of log file, to be parsed and substituted"""
  m = expr.match(l)
  print "Using JGROUPS VIEW line:"
  print "   %s" % l 
  members = m.group(1).strip()
  i = 1
  for m in members.split(','):
    addresses[m.strip()] = "CACHE%s" % i
    new_addresses["CACHE%s" % i] = m.strip()
    i += 1

def help():
  print '''
    INFINISPAN log file fixer.  Makes log files more readable by replacing JGroups addresses with friendly names.
  '''

def usage():
  print '''
  Usage: 
      $ bin/cleanlogs.py <N> <input_file> <output_file>
    OR:
      $ bin/cleanlogs.py <input_file> <output_file> to allow the script to guess which view is most appropriate.

    N: (number) the JGroups VIEW ID to use as the definite list of caches.  Choose a view which has the most complete cache list.
    input_file: path to log file to transform
    output_file: path to result file
  '''

def guess_view(fn):
  """Guesses which view is the most complete, by looking for the view with the largest number of members.  Inaccurate for log files that involve members leaving and others joining at the same time."""
  all_views_re = re.compile('.*Received new cluster view.*\|([0-9]+). \[(.*)\].*')
  views = {}
  with open(fn) as f:
    for l in f:
      m = all_views_re.match(l)
      if m:
        view_num = m.group(1)
        members = m.group(2)
        views[view_num] = as_list(members)
  return views

def most_likely_view(views):
  """Picks the most likely view from a dictionary of views, keyed on view ID.  Returns the view ID."""
  largest_view = -1
  lvid = -1
  for i in views.items():
    if largest_view < len(i[1]):
      largest_view = len(i[1])
      lvid = i[0]
  return lvid

def as_list(string_members):
  """Returns a string of comma-separated member addresses as a list"""
  ml = []
  for m in string_members.split(","):
    ml.append(m.strip())
  return ml
    
def main():
  help()
  ### Get args
  if len(sys.argv) != 4 and len(sys.argv) != 3:
    usage()
    sys.exit(1)

  if len(sys.argv) == 4:
    VIEW_TO_USE = int(sys.argv[1])
    INPUT_FILE = sys.argv[2]
    OUTPUT_FILE = sys.argv[3]
  else:
    INPUT_FILE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]
    views = guess_view(INPUT_FILE)
    VIEW_TO_USE = most_likely_view(views)
    print "Guessing you want view id %s" % VIEW_TO_USE

  expr = re.compile('.*Received new cluster view.*\|%s. \[(.*)\].*' % VIEW_TO_USE)
  find(INPUT_FILE, expr)

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
