package org.infinispan.container.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.FilterSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.PeekableMap;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.impl.PassivationManager;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.infinispan.util.concurrent.DataOperationOrderer.Operation;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

/**
 * Abstract class implemenation for a segmented data container. All methods delegate to
 * {@link #getSegmentForKey(Object)} for methods that don't provide a segment and implementors can provide what
 * map we should look into for a given segment via {@link #getMapForSegment(int)}.
 * @author wburns
 * @since 9.3
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractInternalDataContainer<K, V> implements InternalDataContainer<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   @Inject protected TimeService timeService;
   @Inject protected EvictionManager<K, V> evictionManager;
   @Inject protected InternalExpirationManager<K, V> expirationManager;
   @Inject protected InternalEntryFactory entryFactory;
   @Inject protected ComponentRef<PassivationManager> passivator;
   @Inject protected Configuration configuration;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected DataOperationOrderer orderer;

   protected boolean hasPassivation;

   protected final List<Consumer<Iterable<InternalCacheEntry<K, V>>>> listeners = new CopyOnWriteArrayList<>();

   protected abstract ConcurrentMap<K, InternalCacheEntry<K, V>> getMapForSegment(int segment);
   protected abstract int getSegmentForKey(Object key);

   @Start
   public void start() {
      hasPassivation = configuration.persistence().passivation();
   }

   @Override
   public InternalCacheEntry<K, V> get(int segment, Object k) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> map = getMapForSegment(segment);
      InternalCacheEntry<K, V> e = map != null ? map.get(k) : null;
      if (e != null && e.canExpire()) {
         long currentTimeMillis = timeService.wallClockTime();
         if (e.isExpired(currentTimeMillis) &&
               expirationManager.entryExpiredInMemory(e, currentTimeMillis, false).join() == Boolean.TRUE) {
            e = null;
         } else {
            e.touch(currentTimeMillis);
         }
      }
      return e;
   }

   @Override
   public InternalCacheEntry<K, V> get(Object k) {
      return get(getSegmentForKey(k), k);
   }

   @Override
   public InternalCacheEntry<K, V> peek(int segment, Object k) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> entries = getMapForSegment(segment);
      if (entries != null) {
         if (entries instanceof PeekableMap) {
            return ((PeekableMap<K, InternalCacheEntry<K, V>>) entries).peek(k);
         }
         return entries.get(k);
      }
      return null;
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object k) {
      return peek(getSegmentForKey(k), k);
   }

   @Override
   public boolean touch(int segment, Object k, long currentTimeMillis) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> entries = getMapForSegment(segment);
      if (entries != null) {
         if (trace) {
            log.tracef("Touching key %s in container with time of %d", k, currentTimeMillis);
         }
         if (entries instanceof TouchableMap) {
            return ((TouchableMap) entries).touchKey(k, currentTimeMillis);
         }
         // We use get to also update eviction recency access
         InternalCacheEntry<K, V> entry = entries.get(k);
         if (entry != null) {
            entry.touch(currentTimeMillis);
            return true;
         }
      }
      return false;
   }

   @Override
   public void put(int segment, K k, V v, Metadata metadata, long createdTimestamp, long lastUseTimestamp) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> entries = getMapForSegment(segment);
      if (entries != null) {
         boolean l1Entry = false;
         if (metadata instanceof L1Metadata) {
            metadata = ((L1Metadata) metadata).metadata();
            l1Entry = true;
         }
         InternalCacheEntry<K, V> e = entries.get(k);

         if (trace) {
            log.tracef("Creating new ICE for writing. Existing=%s, metadata=%s, new value=%s", e, metadata, toStr(v));
         }
         final InternalCacheEntry<K, V> copy;
         if (l1Entry) {
            copy = entryFactory.createL1(k, v, metadata);
         } else if (e != null) {
            copy = entryFactory.update(e, v, metadata);
         } else {
            // this is a brand-new entry
            // -1 signals the timestamps should be ignored
            if (createdTimestamp == -1 && lastUseTimestamp == -1) {
               copy = entryFactory.create(k, v, metadata);
            } else {
               copy = entryFactory.create(k, v, metadata, createdTimestamp, metadata.lifespan(),
                                          lastUseTimestamp, metadata.maxIdle());
            }
         }

         if (trace)
            log.tracef("Store %s=%s in container", k, copy);

         entries.put(k, copy);
      } else {
         log.tracef("Insertion attempted for key: %s but there was no map created for it at segment: %d", k, segment);
      }
   }

   @Override
   public void put(K k, V v, Metadata metadata) {
      put(getSegmentForKey(k), k, v, metadata, -1, -1);
   }

   @Override
   public boolean containsKey(int segment, Object k) {
      InternalCacheEntry<K, V> ice = peek(segment, k);
      if (ice != null && ice.canExpire()) {
         long currentTimeMillis = timeService.wallClockTime();
         if (ice.isExpired(currentTimeMillis)) {
            if (expirationManager.entryExpiredInMemory(ice, currentTimeMillis, false).join() == Boolean.TRUE) {
               ice = null;
            }
         }
      }
      return ice != null;
   }

   @Override
   public boolean containsKey(Object k) {
      return containsKey(getSegmentForKey(k), k);
   }

   @Override
   public InternalCacheEntry<K, V> remove(int segment, Object k) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> entries = getMapForSegment(segment);
      if (entries != null) {
         InternalCacheEntry<K, V> e = entries.remove(k);
         if (trace) {
            log.tracef("Removed %s=%s from container", k, e);
         }

         return e == null || (e.canExpire() && e.isExpired(timeService.wallClockTime())) ? null : e;
      }
      return null;
   }

   @Override
   public InternalCacheEntry<K, V> remove(Object k) {
      return remove(getSegmentForKey(k), k);
   }

   @Override
   public CompletionStage<Void> evict(int segment, K key) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> entries = getMapForSegment(segment);
      if (entries == null) {
         return CompletableFutures.completedNull();
      }
      ByRef<CompletionStage<Void>> evictionStageRef = new ByRef<>(CompletableFutures.completedNull());
      entries.computeIfPresent(key, (o, entry) -> {
         // Note this is non blocking but we are invoking it in the ConcurrentMap locked section - so we have to
         // return the value somehow
         // - we don't need an orderer as it is handled in OrderedClusteringDependentLogic
         // - we don't need eviction manager either as it is handled in NotifyHelper
         evictionStageRef.set(handleEviction(entry, null, passivator.running(), null, this, null));
         computeEntryRemoved(o, entry);
         return null;
      });
      return evictionStageRef.get();
   }

   @Override
   public void evict(K key) {
      CompletionStages.join(evict(getSegmentForKey(key), key));
   }

   @Override
   public InternalCacheEntry<K, V> compute(int segment, K key, DataContainer.ComputeAction<K, V> action) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> entries = getMapForSegment(segment);
      return entries != null ? entries.compute(key, (k, oldEntry) -> {
         InternalCacheEntry<K, V> newEntry = action.compute(k, oldEntry, entryFactory);
         if (newEntry == oldEntry) {
            return oldEntry;
         } else if (newEntry == null) {
            computeEntryRemoved(k, oldEntry);
            return null;
         }
         computeEntryWritten(k, newEntry);
         if (trace)
            log.tracef("Store %s in container", newEntry);
         return newEntry;
      }) : null;
   }

   @Override
   public InternalCacheEntry<K, V> compute(K key, DataContainer.ComputeAction<K, V> action) {
      return compute(getSegmentForKey(key), key, action);
   }

   @Override
   public void clear(IntSet segments) {
      segments.forEach((int segment) -> {
         Map<K, InternalCacheEntry<K, V>> map = getMapForSegment(segment);
         if (map != null) {
            map.clear();
         }
      });
   }

   /**
    * This method is invoked every time an entry is written inside a compute block
    * @param key key passed to compute method
    * @param value the new value
    */
   protected void computeEntryWritten(K key, InternalCacheEntry<K, V> value) {
      // Do nothing by default
   }

   /**
    * This method is invoked every time an entry is removed inside a compute block
    * @param key key passed to compute method
    * @param value the old value
    */
   protected void computeEntryRemoved(K key, InternalCacheEntry<K, V> value) {
      // Do nothing by default
   }

   @Override
   public void addRemovalListener(Consumer<Iterable<InternalCacheEntry<K, V>>> listener) {
      listeners.add(listener);
   }

   @Override
   public void removeRemovalListener(Object listener) {
      listeners.remove(listener);
   }

   protected class EntryIterator extends AbstractIterator<InternalCacheEntry<K, V>> {

      private final Iterator<InternalCacheEntry<K, V>> it;

      public EntryIterator(Iterator<InternalCacheEntry<K, V>> it) {
         this.it = it;
      }

      protected InternalCacheEntry<K, V> getNext() {
         boolean initializedTime = false;
         long now = 0;
         while (it.hasNext()) {
            InternalCacheEntry<K, V> entry = it.next();
            if (!entry.canExpire()) {
               if (trace) {
                  log.tracef("Return next entry %s", entry);
               }
               return entry;
            } else {
               if (!initializedTime) {
                  now = timeService.wallClockTime();
                  initializedTime = true;
               }
               if (!entry.isExpired(now) || !expirationManager.entryExpiredInMemoryFromIteration(entry, now)) {
                  if (trace) {
                     log.tracef("Return next entry %s", entry);
                  }
                  return entry;
               } else if (trace) {
                  log.tracef("%s is expired", entry);
               }
            }
         }
         if (trace) {
            log.tracef("Return next null");
         }
         return null;
      }
   }

   protected Caffeine<K, InternalCacheEntry<K, V>> applyListener(Caffeine<K, InternalCacheEntry<K, V>> caffeine,
         DefaultEvictionListener listener, CacheWriter<K, InternalCacheEntry<K, V>> additionalWriter) {
      return caffeine.executor(new WithinThreadExecutor()).writer(new CacheWriter<K, InternalCacheEntry<K, V>>() {
         @Override
         public void write(K key, InternalCacheEntry<K, V> value) {
            if (additionalWriter != null) {
               additionalWriter.write(key, value);
            }
         }

         @Override
         public void delete(K key, InternalCacheEntry<K, V> value, RemovalCause cause) {
            if (additionalWriter != null) {
               additionalWriter.delete(key, value, cause);
            }
            if (cause == RemovalCause.SIZE) {
               listener.onEntryChosenForEviction(key, value);
            }
         }
      }).removalListener(listener);
   }

   static <K, V> Caffeine<K, V> caffeineBuilder() {
      return (Caffeine<K, V>) Caffeine.newBuilder();
   }

   /**
    * Performs the eviction logic, except it doesn't actually remove the entry from the data container.
    *
    * It will acquire the orderer for the key if necessary (not null), passivate the entry, and notify the listeners,
    * all in a non blocking fashion.
    * The caller MUST hold the data container key lock.
    *
    * If the orderer is null, it means a concurrent write/remove is impossible, so we always passivate
    * and notify the listeners.
    *
    * If the orderer is non-null and the self delay is null, when the orderer stage completes
    * we know both the eviction operation removed the entry from the data container and the other operation
    * removed/updated/inserted the entry, but we don't know the order.
    * We don't care about the order for removals, we always skip passivation.
    * We don't care about the order for activations/other evictions (READ) either, we always perform passivation.
    * For writes we want to passivate only if the entry is no longer in the data container, i.e. the eviction
    * removed the entry last.
    *
    * If the self delay is non-null, we may also acquire the orderer before the eviction operation removes the entry.
    * We have to wait for the delay to complete before passivating the entry, but the scenarios are the same.
    *
    * It doesn't make sense to have a null orderer and a non-null self delay.
    * @param entry evicted entry
    * @param orderer used to guarantee ordering between other operations. May be null when an operation is already ordered
    * @param passivator Passivates the entry to the store if necessary
    * @param evictionManager Handles additional eviction logic. May be null if eviction is also not required
    * @param dataContainer container to check if the key has already been removed
    * @param selfDelay if null, the entry was already removed;
    *                  if non-null, completes after the eviction finishes removing the entry
    * @param <K> key type of the entry
    * @param <V> value type of the entry
    * @return stage that when complete all of the eviction logic is complete
    */
   public static <K, V> CompletionStage<Void> handleEviction(InternalCacheEntry<K, V> entry, DataOperationOrderer orderer,
         PassivationManager passivator, EvictionManager<K, V> evictionManager, DataContainer<K, V> dataContainer,
         CompletionStage<Void> selfDelay) {
      K key = entry.getKey();
      CompletableFuture<Operation> future = new CompletableFuture<>();
      CompletionStage<Operation> ordererStage = orderer != null ? orderer.orderOn(key, future) : null;
      if (ordererStage != null) {
         if (trace) {
            log.tracef("Encountered concurrent operation during eviction of %s", key);
         }
         return ordererStage.thenCompose(operation -> {
            if (trace) {
               log.tracef("Concurrent operation during eviction of %s was %s", key, operation);
            }
            switch (operation) {
               case REMOVE:
                  return skipPassivation(orderer, key, future, operation);
               case WRITE:
                  if (dataContainer.containsKey(key)) {
                     if (selfDelay != null) {
                        if (trace) {
                           log.tracef("Delaying check for %s verify if passivation should occur as there was a" +
                                 " concurrent write", key);
                        }
                        return selfDelay.thenCompose(ignore -> {
                           // Recheck the data container after eviction has completed
                           if (dataContainer.containsKey(key)) {
                              return skipPassivation(orderer, key, future, operation);
                           } else {
                              return handleNotificationAndOrderer(key, entry, passivator.passivateAsync(entry), orderer, evictionManager, future);
                           }
                        });
                     }
                     return skipPassivation(orderer, key, future, operation);
                  }
                  //falls through
               default:
                  CompletionStage<Void> passivatedStage = passivator.passivateAsync(entry);
                  // This is a concurrent regular read - in which case we passivate just as normal
                  return handleNotificationAndOrderer(key, entry, passivatedStage, orderer, evictionManager, future);
            }
         });
      }
      return handleNotificationAndOrderer(key, entry, passivator.passivateAsync(entry), orderer, evictionManager, future);
   }

   private static CompletionStage<Void> skipPassivation(DataOperationOrderer orderer, Object key,
         CompletableFuture<Operation> future, Operation op) {
      if (trace) {
         log.tracef("Skipping passivation for key %s due to %s", key, op);
      }
      orderer.completeOperation(key, future, Operation.READ);
      return CompletableFutures.completedNull();
   }

   private static <K, V> CompletionStage<Void> handleNotificationAndOrderer(K key, InternalCacheEntry<K, V> value,
         CompletionStage<Void> stage, DataOperationOrderer orderer, EvictionManager<K, V> evictionManager,
         CompletableFuture<Operation> future) {
      if (evictionManager != null) {
         stage = stage.thenCompose(ignore -> evictionManager.onEntryEviction(Collections.singletonMap(key, value)));
      }
      if (orderer != null) {
         return stage.whenComplete((ignore, ignoreT) -> orderer.completeOperation(key, future, Operation.READ));
      }
      return stage;
   }

   final class DefaultEvictionListener implements RemovalListener<K, InternalCacheEntry<K, V>> {
      Map<Object, CompletableFuture<Void>> ensureEvictionDone = new ConcurrentHashMap<>();

      void onEntryChosenForEviction(K key, InternalCacheEntry<K, V> value) {
         // Schedule an eviction to happen after the key lock is released
         CompletableFuture<Void> future = new CompletableFuture<>();
         ensureEvictionDone.put(key, future);
         handleEviction(value, orderer, passivator.running(), evictionManager, AbstractInternalDataContainer.this,
               future);
      }

      // It is very important that the fact that this method is invoked AFTER the entry has been evicted outside of the
      // lock. This way we can see if the entry has been updated concurrently with an eviction properly
      @Override
      public void onRemoval(K key, InternalCacheEntry<K, V> value, RemovalCause cause) {
         if (cause == RemovalCause.SIZE) {
            CompletableFuture<Void> future = ensureEvictionDone.remove(key);
            if (future != null) {
               future.complete(null);
            }
         }
      }
   }

   /**
    * Returns a new spliterator that will not return entries that have expired.
    * @param spliterator the spliterator to filter expired entries out of
    * @return new spliterator with expired entries filtered
    */
   protected Spliterator<InternalCacheEntry<K, V>> filterExpiredEntries(Spliterator<InternalCacheEntry<K, V>> spliterator) {
      // This way we only read the wall clock time at the beginning
      long accessTime = timeService.wallClockTime();
      return new FilterSpliterator<>(spliterator, expiredIterationPredicate(accessTime));
   }

   /**
    * Returns a predicate that will return false when an entry is expired. This predicate assumes this is invoked from
    * an iteration process.
    * @param accessTime the access time to base expiration off of
    * @return predicate that returns true if an entry is not expired
    */
   protected Predicate<InternalCacheEntry<K, V>> expiredIterationPredicate(long accessTime) {
      return e -> ! e.canExpire() ||
            ! (e.isExpired(accessTime) && expirationManager.entryExpiredInMemoryFromIteration(e, accessTime));
   }
}
