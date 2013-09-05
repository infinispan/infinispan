package org.infinispan.configuration.cache;

import java.util.Properties;

public interface StoreConfigurationChildBuilder<S> extends ConfigurationChildBuilder {

   /**
    * Configuration for the async cache store. If enabled, this provides you with asynchronous
    * writes to the cache store, giving you 'write-behind' caching.
    */
   AsyncStoreConfigurationBuilder<S> async();

   /**
    * SingletonStore is a delegating cache store used for situations when only one instance in a
    * cluster should interact with the underlying store. The coordinator of the cluster will be
    * responsible for the underlying CacheStore. SingletonStore is a simply facade to a real
    * CacheStore implementation. It always delegates reads to the real CacheStore.
    */
   SingletonStoreConfigurationBuilder<S> singleton();

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true. Finally,
    * setting it to true only makes sense if in a clustered environment, and only 'replication' and
    * 'invalidation' cluster modes are supported.
    */
   S fetchPersistentState(boolean b);

   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
    * applied to the cache store. This means that the cache store could become out of sync with the
    * cache.
    */
   S ignoreModifications(boolean b);

   /**
    * If true, purges this cache store when it starts up.
    */
   S purgeOnStartup(boolean b);

   /**
    * If true, when the cache starts, data stored in the cache store will be pre-loaded into memory.
    * This is particularly useful when data in the cache store will be needed immediately after
    * startup and you want to avoid cache operations being delayed as a result of loading this data
    * lazily. Can be used to provide a 'warm-cache' on startup, however there is a performance
    * penalty as startup time is affected by this process.
    */
   S preload(boolean b);

   /**
    * This setting should be set to true when multiple cache instances share the same cache store
    * (e.g., multiple nodes in a cluster using a JDBC-based CacheStore pointing to the same, shared
    * database.) Setting this to true avoids multiple cache instances writing the same modification
    * multiple times. If enabled, only the node where the modification originated will write to the
    * cache store.
    * <p/>
    * If disabled, each individual cache reacts to a potential remote update by storing the data to
    * the cache store. Note that this could be useful if each individual node has its own cache
    * store - perhaps local on-disk.
    */
   S shared(boolean b);

   /**
    * <p>
    * Defines a single property. Can be used multiple times to define all needed properties, but the
    * full set is overridden by {@link #withProperties(java.util.Properties)}.
    * </p>
    * <p>
    * These properties are passed directly to the cache store.
    * </p>
    */
   S addProperty(String key, String value);

   /**
    * Properties passed to the cache store or loader
    */
   S withProperties(Properties p);
}
