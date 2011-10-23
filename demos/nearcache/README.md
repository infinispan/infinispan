Near Cache demo
===============

This is a near cache demo that uses JMS in order to invalidate the near caches.
The demo is formed of two parts:
1. Hot Rod and JMS server that registers a cache listener to send notifications
to registered clients
2. CDI web app that runs a near cache configured with a remote cache store and
a JMS client to listen for notifications

To run the demo, do the following:
1. Start org.infinispan.demo.nearcache.server.MessagingHotRodDemo with the
following system properties:
      -Dorg.hornetq.logger-delegate-factory-class-name=org.hornetq.integration.logging.Log4jLogDelegateFactory
2. Start AS7 instance in lo2:
      ./bin/standalone.sh -Djboss.bind.address=lo2 -Djboss.bind.address.management=lo2
3. Start AS7 instance in lo3:
      ./bin/standalone.sh -Djboss.bind.address=lo3 -Djboss.bind.address.management=lo3
4. Go to nearcache-client/ project and execute:
      mvn clean package jboss-as:deploy -Das.hostname=lo2
      mvn clean package jboss-as:deploy -Das.hostname=lo3
5. Go to [nearcache app in lo2](http://lo2:8080/infinispan-nearcache) and store
two person and address mappings
6. Go to [nearcache app in lo3](http://lo3:8080/infinispan-nearcache) and get
one of the addresses giving a person name
7. Remove a person from either lo2 or lo3 and check whether it's reflected in
 the cached values on the other node

Future improvements
-------------------
* Have per key/cache notifications
* Get rid of JMS once Hot Rod protocol has notifications embedded