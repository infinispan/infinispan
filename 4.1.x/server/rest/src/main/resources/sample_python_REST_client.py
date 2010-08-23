#!/usr/bin/python

#
# Sample python code using the standard http lib only
#

import httplib

## Your Infinispan WAR server host
hostname = "localhost:8080"

#putting data in 
conn = httplib.HTTPConnection(hostname)
data = "SOME DATA HERE !" #could be string, or a file...
conn.request("POST", "/infinispan/rest/Bucket/0", data, {"Content-Type": "text/plain"})
response = conn.getresponse()
print response.status

#getting data out
import httplib
conn = httplib.HTTPConnection(hostname)
conn.request("GET", "/infinispan/rest/Bucket/0")
response = conn.getresponse()
print response.status
print response.read()

## For more information on usagse see http://www.jboss.org/community/wiki/InfinispanRESTserver

