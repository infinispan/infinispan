package org.infinispan.configuration.cache;

import java.util.Properties;

import org.infinispan.commons.configuration.ConfigurationInfo;

/**
 * StoreConfiguration contains generic configuration elements available to all the stores.
 *
 * @author Tristan Tarrant
 * @author Mircea Markus
 * @since 5.2
 */
public interface StoreConfiguration extends ConfigurationInfo {

   /**
    * Configuration for the async cache loader. If enabled, this provides you with asynchronous
    * writes to the cache store, giving you 'write-behind' caching.
    */
   AsyncStoreConfiguration async();

   /**
    * SingletonStore is a delegating cache store used for situations when only one instance in a
    * cluster should interact with the underlying store. The coordinator of the cluster will be
    * responsible for the underlying CacheStore. SingletonStore is a simply facade to a real
    * CacheStore implementation. It always delegates reads to the real CacheStore.
    *
    * @deprecated Singleton writers will be removed in 10.0. If it is desirable that all nodes don't write to the underlying store
    * then a shared store should be used instead, as this only performs store writes at a key's primary owner.
    */
   @Deprecated
   SingletonStoreConfiguration singletonStore();

   /**
    * If true, purges this cache store when it starts up.
    */
   boolean purgeOnStartup();

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true. Finally,
    * setting it to true only makes sense if in a clustered environment, and only 'replication' and
    * 'invalidation' cluster modes are supported.
    */
   boolean fetchPersistentState();

   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
    * applied to the cache store. This means that the cache store could become out of sync with the
    * cache.
    */
   boolean ignoreModifications();

   boolean preload();

   boolean shared();

   boolean transactional();

   int maxBatchSize();

   /**
    * Whether or not this store is configured to be segmented. For a non shared store this means there will be a separate
    * instance of each store for each segment. For shared stores normally this means the store is able to do some
    * optimizations based on the segment (ie. select * from table where segment = $1)
    * @return whether this store is configured to be segmented
    */
   default boolean segmented() {
      return false;
   }

   Properties properties();
}
