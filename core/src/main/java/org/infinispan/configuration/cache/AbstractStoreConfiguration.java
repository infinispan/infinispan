package org.infinispan.configuration.cache;

import java.util.Properties;

public class AbstractStoreConfiguration implements StoreConfiguration {

   private final boolean purgeOnStartup;

   private final boolean fetchPersistentState;
   private final boolean ignoreModifications;
   private final boolean preload;
   private final boolean shared;

   private final Properties properties;
   private final AsyncStoreConfiguration async;

   private final SingletonStoreConfiguration singletonStore;

   public AbstractStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                     AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload,
                                     boolean shared, Properties properties) {
      this.purgeOnStartup = purgeOnStartup;
      this.fetchPersistentState = fetchPersistentState;
      this.ignoreModifications = ignoreModifications;
      this.async = async;
      this.singletonStore = singletonStore;
      this.preload = preload;
      this.shared = shared;
      this.properties = properties;
   }

   /**
    * Configuration for the async cache loader. If enabled, this provides you with asynchronous
    * writes to the cache store, giving you 'write-behind' caching.
    */
   @Override
   public AsyncStoreConfiguration async() {
      return async;
   }

   /**
    * SingletonStore is a delegating store used for situations when only one instance in a
    * cluster should interact with the underlying store. The coordinator of the cluster will be
    * responsible for the underlying CacheStore. SingletonStore is a simply facade to a real
    * CacheStore implementation. It always delegates reads to the real CacheStore.
    */
   @Override
   public SingletonStoreConfiguration singletonStore() {
      return singletonStore;
   }

   /**
    * If true, purges this cache store when it starts up.
    */
   @Override
   public boolean purgeOnStartup() {
      return purgeOnStartup;
   }


   @Override
   public boolean shared() {
      return shared;
   }

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true. Finally,
    * setting it to true only makes sense if in a clustered environment, and only 'replication' and
    * 'invalidation' cluster modes are supported.
    */
   @Override
   public boolean fetchPersistentState() {
      return fetchPersistentState;
   }

   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
    * applied to the cache store. This means that the cache store could become out of sync with the
    * cache.
    */
   @Override
   public boolean ignoreModifications() {
      return ignoreModifications;
   }


   @Override
   public boolean preload() {
      return preload;
   }

   @Override
   public Properties properties() {
      return properties;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof AbstractStoreConfiguration)) return false;

      AbstractStoreConfiguration that = (AbstractStoreConfiguration) o;

      if (fetchPersistentState != that.fetchPersistentState) return false;
      if (ignoreModifications != that.ignoreModifications) return false;
      if (purgeOnStartup != that.purgeOnStartup) return false;
      if (async != null ? !async.equals(that.async) : that.async != null) return false;
      if (properties != null ? !properties.equals(that.properties) : that.properties != null) return false;
      if (singletonStore != null ? !singletonStore.equals(that.singletonStore) : that.singletonStore != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (purgeOnStartup ? 1 : 0);
      result = 31 * result + (fetchPersistentState ? 1 : 0);
      result = 31 * result + (ignoreModifications ? 1 : 0);
      result = 31 * result + (async != null ? async.hashCode() : 0);
      result = 31 * result + (singletonStore != null ? singletonStore.hashCode() : 0);
      result = 31 * result + (properties != null ? properties.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "AbstractStoreConfiguration{" +
            "purgeOnStartup=" + purgeOnStartup +
            ", fetchPersistentState=" + fetchPersistentState +
            ", ignoreModifications=" + ignoreModifications +
            ", async=" + async +
            ", singletonStore=" + singletonStore +
            ", properties=" + properties +
            '}';
   }
}
