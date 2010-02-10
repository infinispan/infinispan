#!/usr/bin/python

#
# Sample python code using the standard memcached library to talk to Infinispan memcached server
# To use it, make sure you install Python memcached client library
# 

import memcache
import time

ip = "127.0.0.1"
port = "12211"
ipaddress = ip + ':' + port
mc = memcache.Client([ipaddress], debug=1)

print "Connecting to {0}".format(ipaddress)

def setAndGet(mc, k, v):
   mc.set(k, v)
   get(mc, k, v)

def get(mc, k, v):
   value = mc.get(k)
   if value == v:
      print "OK"
   else:
      print "FAIL"

def setAndGetNone(mc, k, v, t):
   mc.set(k, v, t)
   time.sleep(t + 2)
   value = mc.get(k)
   if value == None:
      print "OK"
   else:
      print "FAIL"

def delete(mc, k):
   ret = mc.delete(k)
   if ret != 0:
      print "OK"
   else:
      print "FAIL"

print "Testing set/get ['{0}': {1}] ...".format("Simple_Key", "Simple value"),
setAndGet(mc, "Simple_Key", "Simple value")

print "Testing delete ['{0}'] ...".format("Simple_Key"),
delete(mc, "Simple_Key")

print "Testing set/get ['{0}' : {1} : {2}] ...".format("Expiring_Key", 999, 3),
setAndGetNone(mc, "Expiring_Key", 999, 3)

print "Testing increment 3 times ['{0}' : starting at {1} ] ...".format("Incr_Decr_Key", "1"),
mc.set("Incr_Decr_Key", "1")   # note that the key used for incr/decr must be a string.
mc.incr("Incr_Decr_Key")
mc.incr("Incr_Decr_Key")
mc.incr("Incr_Decr_Key")
get(mc, "Incr_Decr_Key", "4")

print "Testing decrement 1 time ['{0}' : starting at {1} ] ...".format("Incr_Decr_Key", "4"),
mc.decr("Incr_Decr_Key")
get(mc, "Incr_Decr_Key", "3")

print "Testing decrement 2 times in one call ['{0}' : {1} ] ...".format("Incr_Decr_Key", "3"),
mc.decr("Incr_Decr_Key", 2)
get(mc, "Incr_Decr_Key", "1")

print "Finally, delete ['{0}'] ...".format("Incr_Decr_Key"),
delete(mc, "Incr_Decr_Key")

## For more information see http://community.jboss.org/wiki/UsingInfinispanMemcachedServer