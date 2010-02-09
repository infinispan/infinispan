#!/usr/bin/python

#
# Sample python code using the standard memcached library to talk to Infinispan memcached server
# To use it, make sure you install Python memcached client library
# This particular script tests that it's writing to the server correctly 
#

import memcache
import time

mc = memcache.Client(['127.0.0.1:11211'], debug=1)

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

print "Testing set ['{0}': {1}] ...".format("Simple_Key", "Simple value"),
set(mc, "Simple_Key", "Simple value")

print "Testing set ['{0}' : {1} : {2}] ...".format("Expiring_Key", 999, 3),
set(mc, "Expiring_Key", 999, 3)

print "Testing increment 3 times ['{0}' : starting at {1} ]".format("Incr_Key", "1")
print "Initialise at {0} ...".format("1"),
set(mc, "Incr_Key", "1")   # note that the key used for incr/decr must be a string.
print "Increment by one ...",
incr(mc, 2, "Incr_Key")
print "Increment again ...",
incr(mc, 3, "Incr_Key")
print "Increment yet again ...",
incr(mc, 4, "Incr_Key")

print "Testing decrement 1 time ['{0}' : starting at {1} ]".format("Decr_Key", "4")
print "Initialise at {0} ...".format("4"),
set(mc, "Decr_Key", "4")
print "Decrement by one ...",
decr(mc, 3, "Decr_Key")

print "Testing decrement 2 times in one call ['{0}' : {1} ]".format("Multi_Decr_Key", "3")
print "Initialise at {0} ...".format("3"),
set(mc, "Multi_Decr_Key", "3")
print "Decrement by 2 ...",
decr(mc, 1, "Multi_Decr_Key", 2)

## For more information see http://community.jboss.org/wiki/UsingInfinispanMemcachedServer
