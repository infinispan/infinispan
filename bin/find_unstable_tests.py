#!/usr/bin/python
import re
import time
import sys
import csv
import argparse
import os.path
import fnmatch


def main(args):
  base_dir = args.dir

  annotated_test_files = []
  
  disabled_test_matcher = re.compile('\s*@Test.*groups\s*=\s*("unstable|Array\("unstable"\))|@Category\(UnstableTest\.class\).*')
  filename_matcher = re.compile('.*Test.(java|scala)')

  for dirpath, dirnames, filenames in os.walk(base_dir):
    for filename in filenames:
      if filename_matcher.match(filename):
        test_file = os.path.join(dirpath, filename)
        with open(test_file) as tf:
          for line in tf:
            if disabled_test_matcher.search(line):
              class_name = os.path.splitext(filename)[0]
              rel_test_file = os.path.relpath(test_file, base_dir)
              annotated_test_files.append((class_name, rel_test_file))
              break

  annotated_test_files=sorted(annotated_test_files)

  csvwriter = csv.writer(sys.stdout, dialect='excel-tab')
  for row in annotated_test_files:
    csvwriter.writerow(row)

def extract_class_name(f):
  return splitext(basename(f))[0]


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("dir", help="base directory", nargs='?', default='.')
  args = parser.parse_args()

  main(args)
  
