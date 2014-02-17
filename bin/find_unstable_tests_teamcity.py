#!/usr/bin/python

"""
  Search JIRA using the restkit library (yum install python-restkit).

  Teamcity REST API documentation: http://confluence.jetbrains.com/display/TCD8/REST+API
"""

import json
import re
from restkit import Resource, BasicAuth, request
from pprint import pprint
import argparse
import datetime
from getpass import getpass
import csv
import sys

default_base_url = 'http://ci.infinispan.org'
default_build_types = ['Master Hotspot JDK6', 'Master Hotspot JDK7', 'Master Unstable Tests JDK6']
default_days = 15


def main(args):
  verbose = args.verbose
  server_base_url = args.base_url
  user = args.user
  password = args.password
  days = args.days
  build_type_names = args.build

  # This sends the user and password with the request.
  url = "%s/guestAuth/app/rest/" % (server_base_url)
  filters = []
  if user:
    auth = BasicAuth(user, password)
    filters = [auth]
    url = "%s/httpAuth/app/rest/" % (server_base_url)

  resource = Resource(url, filters=filters)

  buildTypes = get_json(resource, "buildTypes")
  watched_build_type_ids = [bt['id'] for bt in buildTypes['buildType']
      if bt['name'] in default_build_types]
  if verbose: print("Found build ids: %s" %watched_build_type_ids)

  unstable_tests = []
  for btid in watched_build_type_ids:
    days_delta = datetime.timedelta(days=default_days)
    days_ago = datetime.datetime.utcnow() - days_delta
    date = days_ago.strftime('%Y%m%dT%H%M%S') + '+0000'

    builds_path = 'buildTypes/id:%s/builds' % btid
    builds = get_json(resource, builds_path, locator = build_locator(sinceDate = date, status = 'FAILURE'))
    build_ids = [build['id'] for build in builds['build']]
    if verbose: print("Found build ids for build type %s: %s" % (btid, build_ids))

    for bid in build_ids:
      build_path = "builds/id:%s" % bid
      build = get_json(resource, build_path)
      #pprint(build)
      bname = "%s#%s" % (build['buildType']['name'], build['number'])
      bdate = build['startDate']

      test_occurrences_path = "testOccurrences"
      failed_tests = get_json(resource, test_occurrences_path, locator = build_locator(build = "(id:%s)" % bid, status = 'FAILURE'))
      #pprint(failed_tests)
      if 'testOccurrence' in failed_tests:
        failed_test_names = [test['name'] for test in failed_tests['testOccurrence']]
        if verbose: print("Found failed tests for build %s: %s" % (bid, failed_test_names))
        for test_name in failed_test_names:
          clean_test_name = test_name.replace("TestSuite: ", "")
          unstable_tests.append((extract_class_name(clean_test_name), clean_test_name, bname, bdate))


  unstable_tests = sorted(unstable_tests)

  csvwriter = csv.writer(sys.stdout, dialect='excel-tab')
  for row in unstable_tests:
    csvwriter.writerow(row)


def extract_class_name(test_name):
  match = re.search(r'\w+Test', test_name)
  if match:
    class_name = match.group(0)
  else:
    components = test_name.split('.')
    class_name = components[-2]
  return class_name


def build_locator(**locators):
  return ",".join("%s:%s" %(k, v) for (k, v )in locators.items())

def get_json(resource, path, **params):
  response = resource.get(path, headers={'Accept': 'application/json'},
                          params_dict = params)

  # Most successful responses have an HTTP 200 status
  if response.status_int != 200:
    raise Exception("ERROR: status %s" % response.status_int)

  # Convert the text in the reply into a Python dictionary
  return json.loads(response.body_string())


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("-b", "--base-url", help="base URL", default=default_base_url)
  parser.add_argument("-u", "--user", help="user name")
  parser.add_argument("-p", "--password", help="password")
  parser.add_argument("-d", "--days", help="days to search back", default=default_days)
  parser.add_argument("--build", help="one or more builds to search", nargs='*', action='append', default=default_build_types)
  parser.add_argument("-v", "--verbose", help="print debugging information", action="store_true")
  args = parser.parse_args()

  if args.user and not args.password:
    args.password = getpass()

  main(args)