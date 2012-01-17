Using the HBase cachestore requires access to a functional HBase cluster. 
HBase uses Zookeeper for clustering so you must identify the Zookeeper quorum 
and client port in the HBaseCacheStore loader element in the Infinispan config file.
The properties are hbaseZookeeperQuorum and hbaseZookeeperPropertyClientPort, respectively.
The quorum defaults to localhost and the client port defaults to 2181.

The entry* properties are used to confgure the table that stores the cached
items. The expiration* properties are used to configure the table that maintains expiration
metadata for the cached items. The sharedTable property determines whether or not the cachestore
is shared amongst multiple caches. If it is set to true, then multiple caches store their
items in the same table.

The unit tests use an embedded HBase and ZooKeeper cluster that writes its data to the
local file system (as opposed to HDFS). This is adequate for unit testing functionality,
but integration testing should be done against a "real" HBase cluster before any release.
This can be done for the following classes by setting the "USE_EMBEDDED" field to false:
	HBaseCacheStoreTest.java
	HBaseFacadeTest.java
	HBaseCacheStoreTestStandalone.java
	
TODOs
-Make the HBaseFacade methods use strongly typed classes instead of Map/List/Set of Strings and byte[]s.
-Performance testing and performance optimizations
