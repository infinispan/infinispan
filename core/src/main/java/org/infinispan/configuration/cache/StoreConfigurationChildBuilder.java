package org.infinispan.configuration.cache;

import java.util.Properties;

import org.infinispan.persistence.spi.MarshallableEntry;

public interface StoreConfigurationChildBuilder<S> extends ConfigurationChildBuilder {

   /**
    * Configuration for the async cache store. If enabled, this provides you with asynchronous
    * writes to the cache store, giving you 'write-behind' caching.
    */
   AsyncStoreConfigurationBuilder<S> async();

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true. Finally,
    * setting it to true only makes sense if in a clustered environment.
    *
    * @deprecated since 14.0. This method does nothing. Only the first non shared store that supports publishEntries is used for state transfer.
    */
   @Deprecated(forRemoval=true, since = "14.0")
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
    * If true, this cache store will be only used to write entries.
    */
   S writeOnly(boolean b);

   /**
    * If true, when the cache starts, data stored in the cache store will be pre-loaded into memory.
    * If multiple cache stores are chained, only one of them can have this property enabled.
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
        * If disabled, each individual cache reacts to a potential remote update by storing the data to
    * the cache store. Note that this could be useful if each individual node has its own cache
    * store - perhaps local on-disk.
    */
   S shared(boolean b);

   /**
    * This setting should be set to true when the underlying cache store supports transactions and it is desirable for
    * the underlying store and the cache to remain synchronized. With this enabled any Exceptions thrown whilst writing
    * to the underlying store will result in both the store's and the cache's transaction rollingback.
        * If enabled and this store is shared, then writes to this store will be performed at prepare time of the Infinispan Tx.
    * If an exception is encountered by the store during prepare time, then this will result in the global Tx being
    * rolledback along with this stores writes, otherwise writes to this store will be committed during the commit
    * phase of 2PC. If this is not enabled, then writes to the cache store are performed during the commit phase of a Tx.
    * Note that this requires {@link #shared(boolean)} to be set to true.
    */
   S transactional(boolean b);

   /**
    * The maximum size of a batch to be inserted/deleted from the store. If the value is less than one, then no upper limit is placed on the number of operations in a batch.
    */
   S maxBatchSize(int maxBatchSize);

   /**
    * If true this store should either be non shared (segmenting can be done automatically for non shared stores) or
    * the shared store must have the {@link org.infinispan.persistence.spi.NonBlockingStore.Characteristic#SEGMENTABLE} characteristic.
    * Segmented stores help performance for things that require viewing the entire contents of the store (eg. iteration,
    * stream processing, state transfer, mass indexer). If the store doesn't provide constant time operations for methods
    * such as {@link org.infinispan.persistence.spi.NonBlockingStore#load} or
    * {@link org.infinispan.persistence.spi.NonBlockingStore#write(int, MarshallableEntry)} than segmenting this store could also
    * improve performance of those operations.
    * @param b whether this store should be segmented
    * @return this
    */
   S segmented(boolean b);

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
