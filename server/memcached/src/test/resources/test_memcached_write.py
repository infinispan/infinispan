#!/usr/bin/python

#
# Sample python code using the standard memcached library to talk to Infinispan memcached server
# To use it, make sure you install Python memcached client library
# This particular script tests that it's writing to the one of the clustered servers correctly 
#

__author__    = "Galder Zamarreno"
__version__ = "Infinispan 4.1"
__copyright__ = "Copyright (C) 2010 Red Hat Middleware LLC"
__license__   = "Apache License, 2.0"

import memcache
import time

ip = "127.0.0.1"
port = "11211"
ipaddress = ip + ':' + port
mc = memcache.Client([ipaddress], debug=1)

print "Connecting to {0}".format(ipaddress)

def set(mc, key, val, time = 0):
    ret = mc.set(key, val, time)
    if ret != 0:
        print "OK"
    else:
        print "FAIL: returned {0}".format(ret)

def incr(mc, expected, key, delta = 1):
   ret = mc.incr(key, delta)
   if ret == expected:
      print "OK"
   else:
      print "FAIL: returned {0}".format(ret)

def decr(mc, expected, key, delta = 1):
   ret = mc.decr(key, delta)
   if ret == expected:
      print "OK"
   else:
      print "FAIL: returned {0}".format(ret)

key = "Simple_Key"
value = "Simple value"
print "Testing set ['{0}': {1}] ...".format(key, value),
set(mc, key, value)

key = "Expiring_Key"
value = 999
expiry = 3
print "Testing set ['{0}' : {1} : {2}] ...".format(key, value, expiry),
set(mc, key, value, expiry)

key = "Incr_Key"
value = "1"
print "Testing increment 3 times ['{0}' : starting at {1} ]".format(key, value)
print "Initialise at {0} ...".format(value),
set(mc, key, value)   # note that the key used for incr/decr must be a string.
print "Increment by one ...",
incr(mc, 2, key)
print "Increment again ...",
incr(mc, 3, key)
print "Increment yet again ...",
incr(mc, 4, key)

key = "Decr_Key"
value = "4"
print "Testing decrement 1 time ['{0}' : starting at {1} ]".format(key, value)
print "Initialise at {0} ...".format(value),
set(mc, key, value)
print "Decrement by one ...",
decr(mc, 3, key)

key = "Multi_Decr_Key"
value = "3"
print "Testing decrement 2 times in one call ['{0}' : {1} ]".format(key, value)
print "Initialise at {0} ...".format(value),
set(mc, key, value)
print "Decrement by 2 ...",
decr(mc, 1, "Multi_Decr_Key", 2)

## For more information see http://community.jboss.org/wiki/UsingInfinispanMemcachedServer
