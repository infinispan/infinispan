package org.infinispan.persistence.manager;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.support.DelegatingPersistenceManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

public class PassivationPersistenceManager extends DelegatingPersistenceManager {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final ConcurrentMap<Object, MarshallableEntry> map = new ConcurrentHashMap<>();

   @Inject KeyPartitioner keyPartitioner;

   public PassivationPersistenceManager(PersistenceManager persistenceManager) {
      super(persistenceManager);
   }

   public CompletionStage<Void> passivate(MarshallableEntry marshallableEntry, int segment) {
      Object key = marshallableEntry.getKey();
      if (log.isTraceEnabled()) {
         log.tracef("Storing entry temporarily during passivation for key %s", key);
      }
      map.put(key, marshallableEntry);
      return writeToAllNonTxStores(marshallableEntry, segment, AccessMode.PRIVATE)
            .whenComplete((ignore, t) -> {
               if (t == null) {
                  map.remove(key);
                  if (log.isTraceEnabled()) {
                     log.tracef("Removed temporary entry during passivation for key %s", key);
                  }
               } else {
                  // The store write failed. The entry has already been evicted from the data container, so removing
                  // it from this in-memory map as well would silently lose it (ISPN-18113). Instead we retain it here
                  // so it stays visible to reads, iteration and size, and can be flushed to the store again on a later
                  // passivation of the same key or on shutdown via flushPendingPassivations(). The exception is still
                  // propagated so the caller logs the passivation failure.
                  if (log.isTraceEnabled()) {
                     log.tracef(t, "Retaining entry for key %s in memory after passivation failure", key);
                  }
               }
            });
   }

   /**
    * Attempts to write every entry that is still held in memory because a previous passivation failed to the
    * underlying stores. Entries that are written successfully are removed from the in-memory map; entries that fail
    * again are retained. This is used on shutdown to give quarantined entries a final chance to be persisted once the
    * store has (potentially) recovered.
    *
    * @return a stage that completes once every pending entry has been attempted.
    */
   public CompletionStage<Void> flushPendingPassivations() {
      if (map.isEmpty()) {
         return CompletableFuture.completedFuture(null);
      }
      // Copy the keys so concurrent modifications to the map do not interfere with the iteration.
      AggregateCompletionStage<Void> aggregate = CompletionStages.aggregateCompletionStage();
      for (Object key : new HashSet<>(map.keySet())) {
         MarshallableEntry entry = map.get(key);
         if (entry == null) {
            continue;
         }
         int segment = keyPartitioner.getSegment(key);
         // Swallow individual failures so a store that is still unavailable does not abort the shutdown flush.
         aggregate.dependsOn(writeToAllNonTxStores(entry, segment, AccessMode.PRIVATE)
               .handle((ignore, t) -> {
                  if (t == null) {
                     map.remove(key, entry);
                  } else if (log.isTraceEnabled()) {
                     log.tracef(t, "Unable to flush pending passivation entry for key %s", key);
                  }
                  return null;
               }));
      }
      return aggregate.freeze();
   }

   @Override
   public CompletionStage<Boolean> deleteFromAllStores(Object key, int segment,
                                                       Predicate<? super StoreConfiguration> predicate) {
      // Drop any entry retained after a failed passivation so a removed key cannot be resurrected from the in-memory map.
      map.remove(key);
      return super.deleteFromAllStores(key, segment, predicate);
   }

   @Override
   public CompletionStage<Void> clearAllStores(Predicate<? super StoreConfiguration> predicate) {
      // A clear must also discard entries retained after a failed passivation, otherwise they would survive the clear.
      map.clear();
      return super.clearAllStores(predicate);
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, int segment,
                                                                            boolean localInvocation, boolean includeStores) {
      MarshallableEntry entry = map.get(key);
      if (entry != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Retrieved entry for key %s from temporary passivation map", key);
         }
         return CompletableFuture.completedFuture(entry);
      }
      return super.loadFromAllStores(key, segment, localInvocation, includeStores);
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, boolean localInvocation,
                                                                            boolean includeStores) {
      MarshallableEntry entry = map.get(key);
      if (entry != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Retrieved entry for key %s from temporary passivation map", key);
         }
         return CompletableFuture.completedFuture(entry);
      }
      return super.loadFromAllStores(key, localInvocation, includeStores);
   }

   @Override
   public <K> Publisher<K> publishKeys(Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate) {
      if (map.isEmpty()) {
         return super.publishKeys(filter, predicate);
      }
      Set<K> keys = (Set<K>) new HashSet<>(map.keySet());
      Predicate<K> filterToUse = key -> !keys.contains(key);
      Flowable<K> mapFlowable = Flowable.fromIterable(keys);
      if (filter != null) {
         filterToUse = filterToUse.and(filter);
         mapFlowable = mapFlowable.filter(filter::test);
      }

      return Flowable.concat(
            mapFlowable,
            super.publishKeys(filterToUse, predicate)
      );
   }

   @Override
   public <K> Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter,
                                       Predicate<? super StoreConfiguration> predicate) {
      if (map.isEmpty()) {
         return super.publishKeys(segments, filter, predicate);
      }
      Set<K> keys = (Set<K>) new HashSet<>(map.keySet());
      Predicate<K> filterToUse = key -> !keys.contains(key);
      Flowable<K> mapFlowable = Flowable.fromIterable(keys);
      if (filter != null) {
         filterToUse = filterToUse.and(filter);
         mapFlowable = mapFlowable.filter(filter::test);
      }

      return Flowable.concat(
            mapFlowable,
            super.publishKeys(segments, filterToUse, predicate)
      );
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(boolean fetchValue, boolean fetchMetadata) {
      if (map.isEmpty()) {
         return super.publishEntries(fetchValue, fetchMetadata);
      }
      Map<Object, MarshallableEntry<K, V>> entries = new HashMap(map);
      Predicate<K> filterToUse = key -> !entries.containsKey(key);
      return Flowable.concat(
            Flowable.fromIterable(entries.values()),
            super.publishEntries(filterToUse, fetchValue, fetchMetadata, AccessMode.BOTH)
      );
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
                                                                   boolean fetchMetadata,
                                                                   Predicate<? super StoreConfiguration> predicate) {
      if (map.isEmpty()) {
         return super.publishEntries(filter, fetchValue, fetchMetadata, predicate);
      }
      Map<Object, MarshallableEntry<K, V>> entries = new HashMap(map);
      Predicate<K> filterToUse = key -> !entries.containsKey(key);
      Flowable<MarshallableEntry<K, V>> mapFlowable = Flowable.fromIterable(entries.values());
      if (filter != null) {
         filterToUse = filterToUse.and(filter);
         mapFlowable = mapFlowable.filter(entry -> filter.test(entry.getKey()));
      }

      return Flowable.concat(
            mapFlowable,
            super.publishEntries(filterToUse, fetchValue, fetchMetadata, predicate)
      );
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter,
                                                                   boolean fetchValue, boolean fetchMetadata,
                                                                   Predicate<? super StoreConfiguration> predicate) {
      if (map.isEmpty()) {
         return super.publishEntries(segments, filter, fetchValue, fetchMetadata, predicate);
      }
      Map<Object, MarshallableEntry<K, V>> entries = new HashMap(map);
      Predicate<K> filterToUse = key -> !entries.containsKey(key);
      Flowable<MarshallableEntry<K, V>> mapFlowable = Flowable.fromIterable(entries.values());
      if (filter != null) {
         filterToUse = filterToUse.and(filter);
         mapFlowable = mapFlowable.filter(entry -> filter.test(entry.getKey()));
      }

      return Flowable.concat(
            mapFlowable,
            super.publishEntries(segments, filterToUse, fetchValue, fetchMetadata, predicate)
      );
   }

   @Override
   public CompletionStage<Long> size(Predicate<? super StoreConfiguration> predicate) {
      if (map.isEmpty()) {
         return super.size(predicate);
      }
      // We can't use optimized size and require iteration if we have entries
      return NonBlockingStore.SIZE_UNAVAILABLE_FUTURE;
   }

   public int pendingPassivations() {
      return map.size();
   }
}
