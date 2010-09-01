#!/usr/bin/ruby

#
# Shows how to interact with Infinispan REST api from ruby.
# No special libraries, just standard net/http
#
# Author: Michael Neale
#
require 'net/http'

http = Net::HTTP.new('localhost', 8080)

#Create new entry
http.post('/infinispan-server-rest/rest/___defaultcache/MyKey', 'DATA HERE', {"Content-Type" => "text/plain"})

#get it back
puts http.get('/infinispan-server-rest/rest/___defaultcache/MyKey').body

#use PUT to overwrite
http.put('/infinispan-server-rest/rest/___defaultcache/MyKey', 'MORE DATA', {"Content-Type" => "text/plain"})

#and remove...
http.delete('/infinispan-server-rest/rest/___defaultcache/MyKey')


#and if you want to do json...
require 'rubygems'
require 'json'

#now for fun, lets do some JSON !
data = {:name => "michael", :age => 42 }
http.put('/infinispan-server-rest/rest/___defaultcache/MyKey', data.to_json, {"Content-Type" => "application/json"})
puts "OK !"

## For more information on usagse see http://community.jboss.org/wiki/InfinispanRESTserver









