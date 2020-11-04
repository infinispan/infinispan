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
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.support.DelegatingPersistenceManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

public class PassivationPersistenceManager extends DelegatingPersistenceManager {
   public PassivationPersistenceManager(PersistenceManager persistenceManager) {
      super(persistenceManager);
   }

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final boolean trace = log.isTraceEnabled();

   private final ConcurrentMap<Object, MarshallableEntry> map = new ConcurrentHashMap<>();

   public CompletionStage<Void> passivate(MarshallableEntry marshallableEntry, int segment) {
      Object key = marshallableEntry.getKey();
      if (trace) {
         log.tracef("Storing entry temporarily during passivation for key %s", key);
      }
      map.put(key, marshallableEntry);
      return writeToAllNonTxStores(marshallableEntry, segment, AccessMode.PRIVATE)
            .whenComplete((ignore, ignoreT) -> {
               map.remove(key);
               if (trace) {
                  log.tracef("Removed temporary entry during passivation for key %s", key);
               }
            });
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, int segment,
                                                                            boolean localInvocation, boolean includeStores) {
      MarshallableEntry entry = map.get(key);
      if (entry != null) {
         if (trace) {
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
         if (trace) {
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
      return CompletableFuture.completedFuture(-1L);
   }

   public int pendingPassivations() {
      return map.size();
   }
}
