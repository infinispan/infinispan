package org.infinispan.persistence.manager;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executor;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.KeyFilter;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;

/**
 * Defines the logic for interacting with the chain of external storage.
 *
 * @author Manik Surtani
 * @author Mircea Markus
 * @since 4.0
 */
public interface PersistenceManager extends Lifecycle {

   /**
    * Loads the data from the external store into memory during cache startup.
    */
   public void preload();

   /**
    * Marks the given storage as disabled.
    */
   void disableStore(String storeType);

   <T> Set<T> getStores(Class<T> storeClass);

   Collection<String> getStoresAsString();

   /**
    * Removes the expired entries from all the existing storage.
    */
   void purgeExpired();

   /**
    * Invokes {@link org.infinispan.persistence.spi.AdvancedCacheWriter#clear()} on all the stores that aloes it.
    */
   void clearAllStores(AccessMode mode);

   boolean deleteFromAllStores(Object key, AccessMode mode);

   void processOnAllStores(KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata);

   void processOnAllStores(Executor executor, KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata);

   void processOnAllStores(KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata, AccessMode mode);

   void processOnAllStores(Executor executor, KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata, AccessMode mode);

   MarshalledEntry loadFromAllStores(Object key, InvocationContext context);

   void writeToAllStores(MarshalledEntry marshalledEntry, AccessMode modes);

   /**
    * Returns the store one configured with fetch persistent state, or null if none exist.
    */
   AdvancedCacheLoader getStateTransferProvider();

   int size();

   public static enum AccessMode {
      /**
       * The operation is performed in all {@link org.infinispan.persistence.spi.CacheWriter} or {@link
       * org.infinispan.persistence.spi.CacheLoader}
       */
      BOTH {
         @Override
         protected boolean canPerform(StoreConfiguration configuration) {
            return true;
         }
      },
      /**
       * The operation is performed only in shared configured {@link org.infinispan.persistence.spi.CacheWriter} or
       * {@link org.infinispan.persistence.spi.CacheLoader}
       */
      SHARED {
         @Override
         protected boolean canPerform(StoreConfiguration configuration) {
            return configuration.shared();
         }
      },
      /**
       * The operation is performed only in non-shared {@link org.infinispan.persistence.spi.CacheWriter} or {@link
       * org.infinispan.persistence.spi.CacheLoader}
       */
      PRIVATE {
         @Override
         protected boolean canPerform(StoreConfiguration configuration) {
            return !configuration.shared();
         }
      };

      /**
       * Checks if the operation can be performed in the {@link org.infinispan.persistence.spi.CacheWriter} or {@link
       * org.infinispan.persistence.spi.CacheLoader} configured with the configuration provided.
       *
       * @param configuration the configuration to test.
       * @return {@code true} if the operation can be performed, {@code false} otherwise.
       */
      protected abstract boolean canPerform(StoreConfiguration configuration);
   }

   void setClearOnStop(boolean clearOnStop);

}


