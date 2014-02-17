#!/usr/bin/python

"""
  Search JIRA using the restkit library (yum install python-restkit).

  JIRA REST API documentation: https://docs.atlassian.com/jira/REST/5.0-m5
"""

import json
import re
from restkit import Resource, BasicAuth, request
from pprint import pprint
import argparse
from getpass import getpass
import csv
import sys

default_base_url = 'https://issues.jboss.org'
jql_search = 'project = ISPN AND (component in ("Test Suite - Core", "Test Suite - Server", "Test Suite - Query") OR labels = testsuite_stability) AND status in (Open, "Coding In Progress", Reopened, "Pull Request Sent") ORDER BY priority DESC'


def main(args):
  verbose = args.verbose
  server_base_url = args.url
  user = args.user
  password = args.password

  # This sends the user and password with the request.
  filters = []
  if user:
    auth = BasicAuth(user, password)
    filters = [auth]

  url = "%s/rest/api/latest" % (server_base_url)
  resource = Resource(url, filters=filters)

  issueList = get_json(resource, "search", jql=jql_search, fields="key,issuetype,created,status,summary", expand="renderedFields", maxResults=500)
  if verbose: pprint(issueList)

  tests = []
  for issue in issueList['issues']:
    id = issue['key']
    summary = issue['fields']['summary']
    match = re.search(r'\w+Test', summary)
    if match:
      test = match.group(0)
      tests.append((test, id, summary))

  tests = sorted(tests)

  csvwriter = csv.writer(sys.stdout, dialect='excel-tab')
  for row in tests:
    csvwriter.writerow(row)


def get_json(resource, path, **params):
  response = resource.get(path, headers={'Content-Type': 'application/json'},
                          params_dict = params)

  # Most successful responses have an HTTP 200 status
  if response.status_int != 200:
    raise Exception("ERROR: status %s" % response.status_int)

  # Convert the text in the reply into a Python dictionary
  return json.loads(response.body_string())


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("--url", help="site URL", default=default_base_url)
  parser.add_argument("-u", "--user", help="user name", required = True)
  parser.add_argument("-p", "--password", help="password")
  parser.add_argument("-v", "--verbose", help="print debugging information", action="store_true")
  args = parser.parse_args()

  if args.user and not args.password:
    args.password = getpass()

  main(args)
