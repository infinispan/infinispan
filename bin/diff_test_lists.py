#!/usr/bin/python

"""
  Merge the results of the find_unstable_tests.py, find_unstable_tests_jira.py, and find_unstable_tests_teamcity.py
"""

import argparse
import csv
import os
from pprint import pprint


def parse_tsv(annotations_file, testNameReplacement, verbose):
  tests = dict()
  with open(annotations_file, 'rb') as csvfile:
    reader = csv.reader(csvfile, dialect='excel-tab')
    for row in reader:
      # AsyncDistExtendedStatisticTest	extended-statistics/src/test/java/org/infinispan/stats/simple/AsyncDistExtendedStatisticTest.java
      # AsyncDistExtendedStatisticTest	ISPN-3995	AsyncDistExtendedStatisticTest.testReplaceWithOldVal fails randomly
      # AsyncDistExtendedStatisticTest	org.infinispan.stats.simple.AsyncDistExtendedStatisticTest.testReplaceWithOldVal	2
      if verbose: pprint(row)
      class_name = row[0]
      row[0] = testNameReplacement
      rows = tests.setdefault(class_name, [])
      rows.append(row)

  if verbose: pprint(tests)
  return tests


def print_diffs(target_dict, source1_dict, source2_dict, verbose):
  diffs = []
  for test, rows in sorted(source1_dict.iteritems()):
    if test not in target_dict:
      diffs.append((test, rows))
  rows = sorted(diffs)
  if verbose: pprint(rows)

  for test, rows in diffs:
    print(test)
    for row in rows:
      print("\t%s" % ("\t".join(row)))
    source2_rows = source2_dict.get(test)
    if source2_rows:
      for row in source2_rows:
        print("\t%s" % ("\t".join(row)))

    print('')

def main(args):
  verbose = args.verbose
  annotations_file = args.annotations_file
  jiras_file = args.jira_file
  teamcity_file = args.teamcity_file
  location = args.find_missing
  if verbose: print csv.list_dialects(); print os.getcwd()

  annotations = parse_tsv(annotations_file, "annotation", verbose)
  jiras = parse_tsv(jiras_file, "jira", verbose)
  teamcity_failures = parse_tsv(teamcity_file, "failure", verbose)

  if location == 'jira' or location == 'all':
    print("Tests annotated as unstable or failing in TeamCity missing an issue in JIRA:")
    print_diffs(jiras, annotations, teamcity_failures, verbose)

  if location == 'annotation' or location == 'all':
    print("Tests with a random failure issue in JIRA or failing in TeamCity missing the unstable annotation:")
    print_diffs(annotations, jiras, teamcity_failures, verbose)

  if location == 'teamcity' or location == 'all':
    print("Tests annotated as unstable or with a random failure issue in JIRA but not failing in TeamCity:")
    print_diffs(teamcity_failures, annotations, jiras, verbose)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("-a", "--annotations-file", help="Unstable test annotations file",
                      required=True)
  parser.add_argument("-j", "--jira-file", help="Unstable test JIRAs file", required=True)
  parser.add_argument("-t", "--teamcity-file", help="TeamCity test failures file",
                      required=True)
  parser.add_argument("-v", "--verbose", help="print debugging information",
                      action="store_true")
  parser.add_argument("find_missing",
                      choices=['jira', 'teamcity', 'annotation', 'all'], default='all')
  args = parser.parse_args()
  if args.verbose: pprint(args)

  main(args)