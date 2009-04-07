JARs included in the distribution

1) REQUIRED JARs
----------------

The following JARs are REQUIRED for the proper operation of Infinispan, in addition to the infinispan-core.jar file:

* jcip-annotations.jar - (http://jcip.net) - Annotations used to assert concurrency behaviour of internal classes.

* jgroups.jar (http://jgroups.org) - Group communications library that is the backbone of Infinispan's replication.
  Necessary even when the cache is run in LOCAL mode.

* jboss-common-core.jar - JBoss utilities used by Infinispan.  Version 2.0.5.GA or above needed if run with JDK 6.

* jboss-logging-spi.jar - Required by jboss-common-core.

* jta.jar - JTA interfaces.  Not needed if these are provided elsewhere, e.g., an application server.

2) OPTIONAL JARs
----------------

The following JARs are OPTIONAL, based on the use of certain cache features:

* je.jar - (licenses/LICENSE-bdbje-lib.txt) - Used by the BDBJECacheLoader, and only necessary if you use this cache loader
  implementation.  BerkeleyDB is a fast and efficient filesystem-based database from Oracle.

* c3p0.jar - (http://sourceforge.net/projects/c3p0) - Database connection pooling library.  Optionally used if you wish
  to use the JDBCCacheLoader and are not running within an application server (and hence don't have external connection
  pooling via a DataSource).  See the User Guide on enabling C3P0 connection pooling for the JDBCCacheLoader.

* jdbm.jar - (http://jdbm.sourceforge.net/) - A filesystem-based database similar to BerkeleyDB.  Used by the JdbmCacheLoader.

* amazon-s3.jar - LGPL licensed utilities for communicating with the Amazon S3 service.  Used by the S3CacheLoader.

* commons-httpclientjar - Required by amazon-s3.

* commons-codec.jar - Required by amazon-s3.
  