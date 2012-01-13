#!/usr/bin/python

from __future__ import print_function

import argparse
import fileinput
import re
import sys


def handleMessage(message, filter):
   if filter.search(message):
      print(message, end='')

def main():
   parser = argparse.ArgumentParser("Filter logs")
   parser.add_argument('pattern', nargs=1,
                      help='Filter pattern')
   parser.add_argument('files', metavar='file', nargs='*', default='-',
                      help='Input file')
   args = parser.parse_args()
   #print(args)

   #pattern = sys.argv[1]
   #files = sys.argv[2:]
   pattern = args.pattern[0]
   files = args.files

   # 2011-06-22 17:49:44,732 DEBUG
   # 2011-07-27 11:46:45,282 35195 TRACE
   messageStartFilter = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}( \d+)? (FATAL|ERROR|WARN|INFO|DEBUG|TRACE)')
   messageFilter = re.compile(pattern, re.MULTILINE | re.DOTALL)

   f = fileinput.input(files)
   message = ""

   for line in f:
      if messageStartFilter.match(line):
         handleMessage(message, messageFilter)
         message = line
      else:
         message = message + line

   handleMessage(message, messageFilter)

main()
