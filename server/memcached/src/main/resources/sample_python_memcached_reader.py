#!/usr/bin/python

#
# Sample python code using the standard memcached library to talk to Infinispan memcached server
# To use it, make sure you install Python memcached client library
#

import memcache
import time
from ftplib import print_line

mc = memcache.Client(['127.0.0.1:11211'], debug=0)

print "Testing set ['{0}': {1}] ...".format("Simple_Key", "Simple value"),
#mc.set("Simple_Key", "Simple value")
value = mc.get("Simple_Key")
if value == "Simple value":
   print "OK"
else:
   print "FAIL"

#print "Testing delete ['{0}'] ...".format("Simple_Key"),
#value = mc.delete("Simple_Key")
#if value != 0:
#   print "OK"
#else:
#   print "FAIL"

print "Testing set ['{0}' : {1} : {2}] ...".format("Expiring_Key", 999, 3),
mc.set("Expiring_Key", 999, 3)
#time.sleep(5)
#value = mc.get("Expiring_Key")
#if value == None:
#   print "OK"
#else:
#   print "FAIL"

print "Testing increment 3 times ['{0}' : starting at {1} ] ...".format("Incr_Key", "1"),
mc.set("Incr_Key", "1")   # note that the key used for incr/decr must be a string.
mc.incr("Incr_Key")
mc.incr("Incr_Key")
mc.incr("Incr_Key")
#value = mc.get("Incr_Decr_Key")
#if value == "4":
#   print "OK"
#else:
#   print "FAIL"

print "Testing decrement 1 time ['{0}' : starting at {1} ] ...".format("Decr_Key", "4"),
mc.set("Decr_Key", "4")
mc.decr("Decr_Key")
#value = mc.get("Incr_Decr_Key")
#if value == "3":
#   print "OK"
#else:
#   print "FAIL"

print "Testing decrement 2 times in one call ['{0}' : {1} ] ...".format("Multi_Decr_Key", "3"),
mc.set("Multi_Decr_Key", "3")
mc.decr("Multi_Decr_Key", 2)
#value = mc.get("Incr_Decr_Key")
#if value == "1":
#   print "OK"
#else:
#   print "FAIL"

#print "Finally, delete ['{0}'] ...".format("Incr_Decr_Key"),
#value = mc.delete("Incr_Decr_Key")
#if value != 0:
#   print "OK"
#else:
#   print "FAIL"


## For more information see http://community.jboss.org/wiki/UsingInfinispanMemcachedServer