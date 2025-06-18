package org.infinispan.persistence.manager;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.util.function.TriPredicate;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

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
    * Returns whether the manager is enabled and has at least one store
    */
   boolean hasWriter();

   boolean hasStore(Predicate<StoreConfiguration> test);

   /**
    * Loads the data from the external store into memory during cache startup.
    */
   Flowable<MarshallableEntry<Object, Object>> preloadPublisher();

   /**
    * Marks the given storage as disabled.
    */
   CompletionStage<Void> disableStore(String storeType);

   /**
    * Adds a new store to the cache.
    *
    * @param storeConfiguration the configuration for the store
    * @throws org.infinispan.commons.CacheException if the cache is was not empty
    */
   CompletionStage<Void> addStore(StoreConfiguration storeConfiguration);

   /**
    * Add a {@link StoreChangeListener} to be notified when a store is added or removed dynamically.
    */
   void addStoreListener(StoreChangeListener listener);

   /**
    * Remote a registered {@link StoreChangeListener}
    */
   void removeStoreListener(StoreChangeListener listener);

   interface StoreChangeListener {

      /**
       * Notifies when a store was added or removed dynamically.
       *
       * This method is always invoked with mutual access to any other method in {@link PersistenceManager}.
       * Implementations must only ensure visibility or atomicity of their own variables and operations.
       */
      void storeChanged(PersistenceStatus persistenceStatus);
   }

   <T> Set<T> getStores(Class<T> storeClass);

   Collection<String> getStoresAsString();

   /**
    * Removes the expired entries from all the existing storage.
    */
   CompletionStage<Void> purgeExpired();

   /**
    * Invokes {@link NonBlockingStore#clear()} )} on all the stores that allow it.
    */
   CompletionStage<Void> clearAllStores(Predicate<? super StoreConfiguration> predicate);

   CompletionStage<Boolean> deleteFromAllStores(Object key, int segment, Predicate<? super StoreConfiguration> predicate);

   /**
    * See {@link #publishEntries(Predicate, boolean, boolean, Predicate)}
    */
   default <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(boolean fetchValue, boolean fetchMetadata) {
      return publishEntries(null, fetchValue, fetchMetadata, AccessMode.BOTH);
   }

   /**
    * Returns a publisher that will publish all entries stored by the underlying cache store. Only the first
    * cache store that doesn't have the {@link org.infinispan.persistence.spi.NonBlockingStore.Characteristic#WRITE_ONLY}
    * characteristic will be used. Predicate is applied by the underlying loader in an attempt to improve performance.
    * <p>
    * Caller can tell the store to also fetch the value or metadata. In some cases, this can improve performance. If
    * metadata is not fetched, the publisher may include expired entries.
    * @param filter filter so that only entries whose key matches are returned
    * @param fetchValue whether to fetch value or not
    * @param fetchMetadata whether to fetch metadata or not
    * @param predicate whether a store can be used by publish entries
    * @param <K> key type
    * @param <V> value type
    * @return publisher that will publish entries
    */
   <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
                                                            boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate);

   /**
    * Returns a publisher that will publish entries that map to the provided segments. It will attempt to find the
    * first segmented store if one is available. If not, it will fall back to the first non-segmented store and
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
   <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue,
                                                            boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate);

   /**
    * Returns a publisher that will publish all keys stored by the underlying cache store. Only the first cache store
    * that doesn't have the {@link org.infinispan.persistence.spi.NonBlockingStore.Characteristic#WRITE_ONLY}
    * characteristic will be used. Predicate is applied by the underlying loader in an attempt to improve performance.
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
    * first segmented store if one is available. If not, it will fall back to the first non-segmented store and
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
   <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, boolean localInvocation, boolean includeStores);

   /**
    * Same as {@link #loadFromAllStores(Object, boolean, boolean)} except that the segment of the key is also
    * provided to avoid having to calculate the segment.
    * @param key key to read the entry from
    * @param segment segment the key maps to
    * @param localInvocation whether this invocation is a local invocation. Some loaders may be ignored if it is not local
    * @param includeStores if a loader that is also a store can be loaded from
    * @return entry that maps to the key
    * default implementation invokes {@link #loadFromAllStores(Object, boolean, boolean)} ignoring the segment
    */
   default <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, int segment, boolean localInvocation, boolean includeStores) {
      return loadFromAllStores(key, localInvocation, includeStores);
   }

   /**
    * Returns an approximate count of how many entries are persisted in the given segments.
    * If no store can handle the request for the given mode, a value of <b>-1</b> is returned instead.
    *
    * @param predicate whether a loader can be used
    * @param segments the segments to include
    * @return size or -1 if approximate size couldn't be computed
    */
   CompletionStage<Long> approximateSize(Predicate<? super StoreConfiguration> predicate, IntSet segments);

   default CompletionStage<Long> size() {
       return size(AccessMode.BOTH);
   }

   default CompletionStage<Long> size(IntSet segments) {
      return size(AccessMode.BOTH, segments);
   }

   /**
    * Returns the count of how many entries are persisted in the given segments. If no store can handle the request
    * for the given mode a value of <b>-1</b> is returned instead.
    *
    * @param predicate whether a loader can be used
    * @param segments segments to check
    * @return size or -1 if size couldn't be computed
    */
   CompletionStage<Long> size(Predicate<? super StoreConfiguration> predicate, IntSet segments);

   /**
    * Returns the count of how many entries are persisted. If no store can handle the request for the given mode a
    * value of <b>-1</b> is returned instead.
    * @param predicate whether a loader can be used
    * @return size or -1 if size couldn't be computed
    */
   CompletionStage<Long> size(Predicate<? super StoreConfiguration> predicate);

   enum AccessMode implements Predicate<StoreConfiguration> {
      /**
       * The operation is performed in all {@link NonBlockingStore}s
       */
      BOTH {
         @Override
         public boolean test(StoreConfiguration configuration) {
            return true;
         }
      },
      /**
       * The operation is performed only in shared configured {@link NonBlockingStore}
       */
      SHARED {
         @Override
         public boolean test(StoreConfiguration configuration) {
            return configuration.shared();
         }
      },
      /**
       * The operation is performed only in non-shared {@link NonBlockingStore}
       */
      PRIVATE {
         @Override
         public boolean test(StoreConfiguration configuration) {
            return !configuration.shared();
         }
      },
      /**
       * The operation is performed only in a {@link NonBlockingStore that has async write behind.
       */
      ASYNC {
         @Override
         public boolean test(StoreConfiguration configuration) {
            return configuration.async().enabled();
         }
      },
      /**
       * The operation is performed only in a {@link NonBlockingStore} that doesn't have async write behind.
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
    * <ul>
    *    <li>The store has the {@link org.infinispan.persistence.spi.NonBlockingStore.Characteristic#TRANSACTIONAL} characteristic</li>
    *    <li>The store is configured to be transactional</li>
    *    <li>The cache's TransactionMode === TRANSACTIONAL</li>
    * </ul>
    *
    * @param marshalledEntry the entry to be written to all non-tx stores.
    * @param segment         the segment the entry maps to
    * @param predicate       should we write to a given store
    */
   default CompletionStage<Void> writeToAllNonTxStores(MarshallableEntry marshalledEntry, int segment, Predicate<? super StoreConfiguration> predicate) {
      return writeToAllNonTxStores(marshalledEntry, segment, predicate, 0);
   }

   /**
    * @see #writeToAllNonTxStores(MarshallableEntry, int, Predicate)
    *
    * @param flags Flags used during command invocation
    */
   CompletionStage<Void> writeToAllNonTxStores(MarshallableEntry marshalledEntry, int segment, Predicate<? super StoreConfiguration> predicate, long flags);

   /**
    * Perform the prepare phase of 2PC on all Tx stores.
    *
    * @param txInvocationContext the tx invocation containing the modifications
    * @param predicate should we prepare on a given store
    * @throws PersistenceException if an error is encountered at any of the underlying stores.
    */
   CompletionStage<Void> prepareAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
                           Predicate<? super StoreConfiguration> predicate) throws PersistenceException;

   /**
    * Perform the commit operation for the provided transaction on all Tx stores.
    *
    * @param txInvocationContext the transactional context to be committed.
    * @param predicate should we commit each store
    */
   CompletionStage<Void> commitAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
         Predicate<? super StoreConfiguration> predicate);

   /**
    * Perform the rollback operation for the provided transaction on all Tx stores.
    *
    * @param txInvocationContext the transactional context to be rolledback.
    * @param predicate should we rollback each store
    */
   CompletionStage<Void> rollbackAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
         Predicate<? super StoreConfiguration> predicate);

   /**
    * Writes the values modified from a put map command to the stores.
    * @param putMapCommand the put map command to write values from
    * @param ctx context to lookup entries
    * @param commandKeyPredicate predicate to control if a key/command combination should be accepted
    * @return a stage of how many writes were performed
    */
   CompletionStage<Long> writeMapCommand(PutMapCommand putMapCommand, InvocationContext ctx,
         BiPredicate<? super PutMapCommand, Object> commandKeyPredicate);

   /**
    * Writes a batch for the given modifications in the transactional context
    * @param invocationContext transactional context
    * @param commandKeyPredicate predicate to control if a key/value/command combination should be accepted
    * @return a stage of how many writes were performed
    */
   CompletionStage<Long> performBatch(TxInvocationContext<AbstractCacheTransaction> invocationContext,
         TriPredicate<? super WriteCommand, Object, MVCCEntry<?, ?>> commandKeyPredicate);

   /**
    * Writes the entries to the stores that pass the given predicate
    * @param iterable entries to write
    * @param predicate predicate to test for a store
    * @param <K> key type
    * @param <V> value type
    * @return a stage that when complete the values were written
    */
   <K, V> CompletionStage<Void> writeEntries(Iterable<MarshallableEntry<K, V>> iterable,
         Predicate<? super StoreConfiguration> predicate);

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
   default CompletionStage<Boolean> addSegments(IntSet segments) {
      return CompletableFutures.completedTrue();
   }

   /**
    * Notifies any underlying segmented stores that a given segment is no longer owned by this cache and allowing
    * it to remove the given segments and release resources related to it.
    * <p>
    * This only affects stores that are not shared as shared stores have to keep all segments running at all times
    * <p>
    * This method returns true if all stores were able to handle the removed segments. That is that either there are no
    * stores or that all the configured stores are segmented. Note that configured loaders do not affect the return
    * value.
    * @param segments segments this cache no longer owns
    * @return false if a configured store couldn't remove configured segments
    */
   default CompletionStage<Boolean> removeSegments(IntSet segments) {
      return CompletableFutures.completedTrue();
   }

   /**
    * @return true if no writable {@link NonBlockingStore} instances have been configured.
    */
   boolean isReadOnly();
}
