package org.infinispan.persistence.manager;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.transaction.Transaction;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.BatchModification;

/**
 * Defines the logic for interacting with the chain of external storage.
 *
 * @author Manik Surtani
 * @author Mircea Markus
 * @since 4.0
 */
public interface PersistenceManager extends Lifecycle {

   /**
    * @return true if all entries from the store have been inserted to the cache. If the persistence/preload
    * is disabled or eviction limit was reached when preloading, returns false.
    */
   boolean isPreloaded();

   /**
    * Loads the data from the external store into memory during cache startup.
    */
   void preload();

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

   /**
    * Returns the store one configured with fetch persistent state, or null if none exist.
    */
   AdvancedCacheLoader getStateTransferProvider();

   int size();

   enum AccessMode {
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

   /**
    * Write to all stores that are not transactional. A store is considered transactional if all of the following are true:
    *
    * <p><ul>
    *    <li>The store implements {@link org.infinispan.persistence.spi.TransactionalCacheWriter}</li>
    *    <li>The store is configured to be transactional</li>
    *    <li>The cache's TransactionMode === TRANSACTIONAL</li>
    * </ul></p>
    *
    * @param marshalledEntry the entry to be written to all non-tx stores.
    * @param modes           the type of access to the underlying store.
    */
   void writeToAllNonTxStores(MarshalledEntry marshalledEntry, AccessMode modes);

   /**
    * @see #writeToAllNonTxStores(MarshalledEntry, AccessMode)
    *
    * @param flags Flags used during command invocation
    */
   void writeToAllNonTxStores(MarshalledEntry marshalledEntry, AccessMode modes, long flags);

   /**
    * Perform the prepare phase of 2PC on all Tx stores.
    *
    * @param transaction the current transactional context.
    * @param batchModification an object containing the write/remove operations required for this transaction.
    * @param accessMode the type of access to the underlying store.
    * @throws PersistenceException if an error is encountered at any of the underlying stores.
    */
   void prepareAllTxStores(Transaction transaction, BatchModification batchModification,
                           AccessMode accessMode) throws PersistenceException;

   /**
    * Perform the commit operation for the provided transaction on all Tx stores.
    *
    * @param transaction the transactional context to be committed.
    * @param accessMode the type of access to the underlying store.
    */
   void commitAllTxStores(Transaction transaction, AccessMode accessMode);

   /**
    * Perform the rollback operation for the provided transaction on all Tx stores.
    *
    * @param transaction the transactional context to be rolledback.
    * @param accessMode the type of access to the underlying store.
    */
   void rollbackAllTxStores(Transaction transaction, AccessMode accessMode);

   /**
    * Write all entries to the underlying non-transactional stores as a single batch.
    *
    * @param entries a List of MarshalledEntry to be written to the store.
    * @param accessMode the type of access to the underlying store.
    * @param flags Flags used during command invocation
    */
   void writeBatchToAllNonTxStores(Iterable<MarshalledEntry> entries, AccessMode accessMode, long flags);

   /**
    * Remove all entries from the underlying non-transactional stores as a single batch.
    *
    * @param entries a List of Keys to be removed from the store.
    * @param accessMode the type of access to the underlying store.
    * @param flags Flags used during command invocation
    */
   void deleteBatchFromAllNonTxStores(Iterable<Object> keys, AccessMode accessMode, long flags);
}
