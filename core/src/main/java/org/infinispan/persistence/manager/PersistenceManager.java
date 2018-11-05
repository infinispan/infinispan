package org.infinispan.persistence.manager;

import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.BatchModification;
import org.reactivestreams.Publisher;

/**
 * Defines the logic for interacting with the chain of external storage.
 *
 * @author Manik Surtani
 * @author Mircea Markus
 * @since 4.0
 */
public interface PersistenceManager extends Lifecycle {

   boolean isEnabled();
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
   void clearAllStores(Predicate<? super StoreConfiguration> predicate);

   boolean deleteFromAllStores(Object key, int segment, Predicate<? super StoreConfiguration> predicate);

   /**
    * See {@link #publishEntries(Predicate, boolean, boolean, Predicate)}
    */
   default <K, V> Publisher<MarshalledEntry<K, V>> publishEntries(boolean fetchValue, boolean fetchMetadata) {
      return publishEntries(null, fetchValue, fetchMetadata, AccessMode.BOTH);
   }

   /**
    * Returns a publisher that will publish all entries stored by the underlying cache store. Only the first
    * cache store that implements {@link AdvancedCacheLoader} will be used. Predicate is applied by the underlying
    * loader in a best attempt to improve performance.
    * <p>
    * Caller can tell the store to also fetch the value or metadata. In some cases this can improve performance. If
    * metadata is not fetched the publisher may include expired entries.
    * @param filter filter so that only entries whose key matches are returned
    * @param fetchValue whether to fetch value or not
    * @param fetchMetadata whether to fetch metadata or not
    * @param predicate whether a store can be used by publish entries
    * @param <K> key type
    * @param <V> value type
    * @return publisher that will publish entries
    */
   <K, V> Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
         boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate);

   /**
    * Returns a publisher that will publish entries that map to the provided segments. It will attempt to find the
    * first segmented store if one is available. If not it will fall back to the first non segmented store and
    * filter out entries that don't map to the provided segment.
    * @param segments only entries that map to these segments are processed
    * @param filter filter so that only entries whose key matches are returned
    * @param fetchValue whether to fetch value or not
    * @param fetchMetadata whether to fetch metadata or not
    * @param predicate whether a store can be used by publish entries
    * @param <K> key type
    * @param <V> value type
    * @return publisher that will publish entries belonging to the given segments
    */
   <K, V> Publisher<MarshalledEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue,
         boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate);

   /**
    * Returns a publisher that will publish all keys stored by the underlying cache store. Only the first cache store
    * that implements {@link AdvancedCacheLoader} will be used. Predicate is applied by the underlying
    * loader in a best attempt to improve performance.
    * <p>
    * This method should be preferred over {@link #publishEntries(Predicate, boolean, boolean, Predicate)} when only
    * keys are desired as many stores can do this in a significantly more performant way.
    * <p>
    * This publisher will never return a key which belongs to an expired entry
    * @param filter filter so that only keys which match are returned
    * @param predicate access mode to choose what type of loader to use
    * @param <K> key type
    * @return publisher that will publish keys
    */
   <K> Publisher<K> publishKeys(Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate);

   /**
    * Returns a publisher that will publish keys that map to the provided segments. It will attempt to find the
    * first segmented store if one is available. If not it will fall back to the first non segmented store and
    * filter out entries that don't map to the provided segment.
    * <p>
    * This method should be preferred over {@link #publishEntries(IntSet, Predicate, boolean, boolean, Predicate)}
    * when only keys are desired as many stores can do this in a significantly more performant way.
    * <p>
    * This publisher will never return a key which belongs to an expired entry
    * @param segments only keys that map to these segments are processed
    * @param filter filter so that only keys which match are returned
    * @param predicate access mode to choose what type of loader to use
    * @param <K> key type
    * @return publisher that will publish keys belonging to the given segments
    */
   <K> Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate);

   /**
    * Loads an entry from the persistence store for the given key. The returned value may be null. This value
    * is guaranteed to not be expired when it was returned.
    * @param key key to read the entry from
    * @param localInvocation whether this invocation is a local invocation. Some loaders may be ignored if it is not local
    * @param includeStores if a loader that is also a store can be loaded from
    * @return entry that maps to the key
    */
   MarshalledEntry loadFromAllStores(Object key, boolean localInvocation, boolean includeStores);

   /**
    * Same as {@link #loadFromAllStores(Object, boolean, boolean)} except that the segment of the key is also
    * provided to avoid having to calculate the segment.
    * @param key key to read the entry from
    * @param segment segment the key maps to
    * @param localInvocation whether this invocation is a local invocation. Some loaders may be ignored if it is not local
    * @param includeStores if a loader that is also a store can be loaded from
    * @return entry that maps to the key
    * @implSpec default implementation invokes {@link #loadFromAllStores(Object, boolean, boolean)} ignoring the segment
    */
   default MarshalledEntry loadFromAllStores(Object key, int segment, boolean localInvocation, boolean includeStores) {
      return loadFromAllStores(key, localInvocation, includeStores);
   }

   /**
    * Returns the store one configured with fetch persistent state, or null if none exist.
    */
   AdvancedCacheLoader getStateTransferProvider();

   default int size() {
      return size(AccessMode.BOTH);
   }

   /**
    * Returns the count of how many entries are persisted. If no store can handle the request for the given mode a
    * value of <b>-1</b> is returned instead.
    * @param predicate whether a loader can be used
    * @return size or -1 if size couldn't be computed
    */
   int size(Predicate<? super StoreConfiguration> predicate);

   /**
    * Returns the count of how many entries are persisted within the given segments. The returned value will always
    * be 0 or greater.
    * @param segments which segments to count entries from
    * @return how many entries are in the store which map to the given segments
    */
   int size(IntSet segments);

   enum AccessMode implements Predicate<StoreConfiguration> {
      /**
       * The operation is performed in all {@link org.infinispan.persistence.spi.CacheWriter} or {@link
       * org.infinispan.persistence.spi.CacheLoader}
       */
      BOTH {
         @Override
         public boolean test(StoreConfiguration configuration) {
            return true;
         }
      },
      /**
       * The operation is performed only in shared configured {@link org.infinispan.persistence.spi.CacheWriter} or
       * {@link org.infinispan.persistence.spi.CacheLoader}
       */
      SHARED {
         @Override
         public boolean test(StoreConfiguration configuration) {
            return configuration.shared();
         }
      },
      /**
       * The operation is performed only in non-shared {@link org.infinispan.persistence.spi.CacheWriter} or {@link
       * org.infinispan.persistence.spi.CacheLoader}
       */
      PRIVATE {
         @Override
         public boolean test(StoreConfiguration configuration) {
            return !configuration.shared();
         }
      },
      /**
       * The operation is performed only in a {@link org.infinispan.persistence.spi.CacheWriter} or {@link
       * org.infinispan.persistence.spi.CacheLoader} that has async write behind.
       */
      ASYNC {
         @Override
         public boolean test(StoreConfiguration configuration) {
            return configuration.async().enabled();
         }
      },
      /**
       * The operation is performed only in a {@link org.infinispan.persistence.spi.CacheWriter} or {@link
       * org.infinispan.persistence.spi.CacheLoader} that doesn't have async write behind.
       */
      NOT_ASYNC {
         @Override
         public boolean test(StoreConfiguration configuration) {
            return !configuration.async().enabled();
         }
      },
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
    * @param segment         the segment the entry maps to
    * @param predicate       should we write to a given store
    */
   void writeToAllNonTxStores(MarshalledEntry marshalledEntry, int segment, Predicate<? super StoreConfiguration> predicate);

   /**
    * @see #writeToAllNonTxStores(MarshalledEntry, int, Predicate)
    *
    * @param flags Flags used during command invocation
    */
   void writeToAllNonTxStores(MarshalledEntry marshalledEntry, int segment, Predicate<? super StoreConfiguration> predicate, long flags);

   /**
    * Perform the prepare phase of 2PC on all Tx stores.
    *
    * @param transaction the current transactional context.
    * @param batchModification an object containing the write/remove operations required for this transaction.
    * @param predicate should we prepare on a given store
    * @throws PersistenceException if an error is encountered at any of the underlying stores.
    */
   void prepareAllTxStores(Transaction transaction, BatchModification batchModification,
         Predicate<? super StoreConfiguration> predicate) throws PersistenceException;

   /**
    * Perform the commit operation for the provided transaction on all Tx stores.
    *
    * @param transaction the transactional context to be committed.
    * @param predicate should we commit each store
    */
   void commitAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate);

   /**
    * Perform the rollback operation for the provided transaction on all Tx stores.
    *
    * @param transaction the transactional context to be rolledback.
    * @param predicate should we rollback each store
    */
   void rollbackAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate);

   /**
    * Write all entries to the underlying non-transactional stores as a single batch.
    *
    * @param entries a List of MarshalledEntry to be written to the store.
    * @param predicate whether a given store should write the entry
    * @param flags Flags used during command invocation
    */
   void writeBatchToAllNonTxStores(Iterable<MarshalledEntry> entries, Predicate<? super StoreConfiguration> predicate, long flags);

   /**
    * Remove all entries from the underlying non-transactional stores as a single batch.
    *
    * @param keys a List of Keys to be removed from the store.
    * @param predicate whether a given store should delete the entries
    * @param flags Flags used during command invocation
    */
   void deleteBatchFromAllNonTxStores(Iterable<Object> keys, Predicate<? super StoreConfiguration> predicate, long flags);


   /**
    * @return true if all configured stores are available and ready for read/write operations.
    */
   boolean isAvailable();

   /**
    * Notifies any underlying segmented stores that the segments provided are owned by this cache and to start/configure
    * any underlying resources required to handle requests for entries on the given segments.
    * <p>
    * This only affects stores that are not shared as shared stores have to keep all segments running at all times
    * <p>
    * This method returns true if all stores were able to handle the added segments. That is that either there are no
    * stores or that all the configured stores are segmented. Note that configured loaders do not affect the return
    * value.
    * @param segments segments this cache owns
    * @return false if a configured store couldn't configure newly added segments
    */
   default boolean addSegments(IntSet segments) {
      return true;
   }

   /**
    * Notifies any underlying segmented stores that a given segment is no longer owned by this cache and allowing
    * it to remove the given segments and release resources related to it.
    * <p>
    * This only affects stores that are not shared as shared stores have to keep all segments running at all times
    * <p>
    * This method returns true if all stores were able to handle the added segments. That is that either there are no
    * stores or that all the configured stores are segmented. Note that configured loaders do not affect the return
    * value.
    * @param segments segments this cache no longer owns
    * @return false if a configured store couldn't remove configured segments
    */
   default boolean removeSegments(IntSet segments) {
      return true;
   }
}
