#!/usr/bin/python

import re
import subprocess

addresses = []

def find(expr):
  f = open("infinispan.log")
  try:
    for l in f:
      if expr.match(l):
        handle(l, expr)
        break
  finally:
    f.close()

def handle(l, expr):
 # print l
  m = expr.match(l)
  members = m.group(1).strip()
  for m in members.split(','):
    addresses.append(m.strip())


VIEW_TO_USE = '3'

expr = re.compile('.*Received new cluster view.*\|%s. \[(.*)\].*' % VIEW_TO_USE)
find(expr)
#print 'Addresses = %s' % addresses
i=1
sed = "sed "
for a in addresses:
  sed += "-e 's/%s/%s/g' " % (a, "CACHE%s" % i)
  i += 1

sed += " infinispan.log > infinispan0.log"

print """
  
  INFINISPAN Log file fixer.  Makes log files more readable by replacing ugly JGroups addresses to more friendly CACHE1, CACHE2, etc addresses.

  Usage:

    $ bin/fixlogs.py

  TODO: be able to specify which view to selec as the correct one, and to specify input and output files.

""" 

print "TODO execute this sed script automatically!"

print sed
print "\n\n\n"

#subprocess.call(sed)


