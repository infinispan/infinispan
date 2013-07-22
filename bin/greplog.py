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

   messageStartFilter = re.compile('.* (FATAL|ERROR|WARN|INFO|DEBUG|TRACE)')
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
