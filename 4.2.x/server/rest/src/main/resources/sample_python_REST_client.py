#!/usr/bin/python

#
# Sample python code using the standard http lib only
#

import httplib

## Your Infinispan WAR server host
hostname = "localhost:8080"
webapp_name = "infinispan-server-rest"
cache_name = "___defaultcache"
key = "my_key"

#putting data in 
print "Storing data on server %s under key [%s] over REST" % (hostname, key)
try:
  conn = httplib.HTTPConnection(hostname)
  data = "This is some test data." #could be string, or a file...
  conn.request("POST", "/%s/rest/%s/%s" % (webapp_name, cache_name, key), data, {"Content-Type": "text/plain"})
  response = conn.getresponse()
  print "HTTP status: %s" % response.status
except:
  print "Unable to connect to the REST server on %s. Is it running?" % hostname  

#getting data out
print "Retrieving data from server %s under key [%s]" % (hostname, key)
try:
  conn = httplib.HTTPConnection(hostname)
  conn.request("GET", "/%s/rest/%s/%s" % (webapp_name, cache_name, key))
  response = conn.getresponse()
  print "HTTP status: %s" % response.status
  print "Value retrieved: %s" % response.read()
except:
  print "Unable to connect to the REST server on %s.  Is it running?" % hostname

## For more information on usage see http://community.jboss.org/wiki/InfinispanRESTserver


