#!/usr/bin/python

#
# Sample python code using the standard memcached library to talk to Infinispan memcached server
# To use it, make sure you install Python memcached client library
# This particular script tests that it's reading from one of the clustered servers correctly
#

__author__    = "Galder Zamarreno"
__version__ = "Infinispan 4.1"
__copyright__ = "Copyright (C) 2010 Red Hat Middleware LLC"
__license__   = "Apache License, 2.0"

import memcache
import time

ip = "127.0.0.1"
port = "11311"
ipaddress = ip + ':' + port
mc = memcache.Client([ipaddress], debug=1)

print "Connecting to {0}".format(ipaddress)

def get(mc, key, expected) :
   value = mc.get(key)
   if value == expected:
      print "OK"
   else:
      print "FAIL"

def getNone(mc, key):
   value = mc.get(key)
   if value == None:
      print "OK"
   else:
      print "FAIL"

key = "Simple_Key"
expected = "Simple value"
print "Testing get ['{0}'] should return {1} ...".format(key, expected),
get(mc, key, expected)

key = "Expiring_Key"
print "Testing get ['{0}'] should return nothing...".format(key),
time.sleep(5)
getNone(mc, key)

key = "Incr_Key"
expected = "4"
print "Testing get ['{0}'] should return {1} ...".format(key, expected),
get(mc, key, expected)

key = "Decr_Key"
expected = "3"
print "Testing get ['{0}'] should return {1} ...".format(key, expected),
get(mc, key, expected)

key = "Multi_Decr_Key"
expected = "1"
print "Testing get ['{0}'] should return {1} ...".format(key, expected),
get(mc, key, expected)

## For more information see http://community.jboss.org/wiki/UsingInfinispanMemcachedServer
