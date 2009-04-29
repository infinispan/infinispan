JARs included in the distribution

REQUIRED JARs
-------------

The following JARs are REQUIRED for the proper operation of Infinispan, in addition to the infinispan-core.jar file:

* jcip-annotations.jar - (http://jcip.net) - Annotations used to assert concurrency behaviour of internal classes.

* jgroups.jar (http://jgroups.org) - Group communications library that is the backbone of Infinispan's replication.
  Necessary even when the cache is run in LOCAL mode.

* jboss-common-core.jar - JBoss utilities used by Infinispan.  Version 2.0.5.GA or above needed if run with JDK 6.

* jboss-logging-spi.jar - Required by jboss-common-core.

* jta.jar - JTA interfaces.  Not needed if these are provided elsewhere, e.g., an application server.

Further, each additional module that you use - for example, cachestore/jdbc or tree - may have additional dependencies.

  