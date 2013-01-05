#!/usr/bin/env python

"""
Sample Python-based memcached command-line application.  Makes use of Danga's 'memcache' 
Python module to communicate with memcached.

Overview
========

See U{the MemCached homepage<http://www.danga.com/memcached>} for more about memcached, and 
U{Infinispan's project page<http://www.infinispan.org>} for more information about Infinispan.

Usage summary
=============

This command-line script is operated as follows:

  $ ./memcached_client.py GET <key> 
  
Would return an entry under key from the memcached server.

  $ ./memcached_client.py PUT <key> <value> 
  
Places a value in the memcached server.

  $ ./memcached_client.py REMOVE <key>

Removes an entry on the server.

Server endpoints
================

This script can connect to a number of server endpoints, and randomly selects an endpoint to use
with each invocation.  Please edit this script to edit the server endpoint list.
"""

__author__    = "Manik Surtani"
__version__ = "Infinispan 4.1"
__copyright__ = "Copyright (C) 2010 Red Hat Middleware LLC"
__license__   = "LGPL"

import memcache
import math
import random
import sys

endpoints = ["127.0.0.1:11211"]

def connect():
  ipaddress = choose_random_endpoint()
  mc = memcache.Client([ipaddress], debug=1)
  print "Connecting to endpoint {0}".format(ipaddress)
  return mc

def put(k, v):
  conn = connect()
  conn.set(k, v)

def get(k):
  return connect().get(k)
  
def remove(k):
  connect().delete(k)

def usage(msg = None):
  if msg:
    print 'Error: %s' % msg
  print '''
  Usage:
    memcached_client.py [GET|PUT|REMOVE] <key> <value>

      GET: Retrieves a value from Infinispan.  Key is required.
      PUT: Stores a value in Infinispan.  Key and Value are required.
      REMOVE: Removes an entry from Infinispan. Key is required.
      
'''
  sys.exit(1)

def main():
  if len(sys.argv) < 3:
    usage(msg = "Not enough parameters!")
  cmd = sys.argv[1]
  key = sys.argv[2]
  
  cmd = cmd.strip().upper()
  if cmd == "GET":
    retval = get(key)
    print "The value of [%s] is [%s]" % (key, retval)
  elif cmd == "REMOVE":
    remove(key)
    print "Ok, removed entry under key [%s]" % key
  elif cmd == "PUT":
    if len(sys.argv) < 4:
      usage(msg = "Not enough parameters!")
    value = sys.argv[3]
    put(key, value)
    print "Ok, set value of [%s] as [%s]" % (key, value)
  else:
    usage(msg = "Unknown command %s" % cmd)

def choose_random_endpoint():
  return endpoints[random.randrange(0, len(endpoints))]

if __name__ == "__main__":
  main()
