package org.infinispan.container.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.lang.invoke.MethodHandles;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.FilterSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.PeekableMap;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.NonBlockingOrderer;
import org.infinispan.util.concurrent.NonBlockingOrderer.OPERATION;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

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
   @Inject protected EvictionManager evictionManager;
   @Inject protected InternalExpirationManager<K, V> expirationManager;
   @Inject protected InternalEntryFactory entryFactory;
   @Inject protected ComponentRef<PassivationManager> passivator;
   @Inject protected Configuration configuration;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected NonBlockingOrderer orderer;

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
            log.tracef("Store %s in container", copy);

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
            log.tracef("Removed %s from container", e);
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
      if (entries != null) {
         ByRef<CompletionStage<Void>> byRef = new ByRef<>(null);
         entries.computeIfPresent(key, (o, entry) -> {
            // Note this is non blocking and we ignore the return value
            // - we don't need an orderer as it is handled in OrderedClusteringDependentLogic
            // - we don't need eviction manager either as it is handled in NotifyHelper
            byRef.set(handleEviction(key, entry, null, passivator.running(), null, this));
            computeEntryRemoved(o, entry);
            return null;
         });
         CompletionStage<Void> stage = byRef.get();
         if (stage != null) {
            return stage;
         }
      }
      return CompletableFutures.completedNull();
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

   @Override
   public Set<K> keySet() {
      // This automatically immutable
      return new AbstractSet<K>() {
         @Override
         public boolean contains(Object o) {
            return containsKey(o);
         }

         @Override
         public Iterator<K> iterator() {
            return new IteratorMapper<>(iteratorIncludingExpired(), Map.Entry::getKey);
         }

         @Override
         public int size() {
            return AbstractInternalDataContainer.this.size();
         }

         @Override
         public Spliterator<K> spliterator() {
            return Spliterators.spliterator(this, Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.CONCURRENT);
         }
      };
   }

   @Override
   public Collection<V> values() {
      return new Values();
   }

   /**
    * Minimal implementation needed for unmodifiable Collection
    * @deprecated This is to removed when {@link #entrySet()} is removed
    */
   @Deprecated
   protected class Values extends AbstractCollection<V> {
      @Override
      public Iterator<V> iterator() {
         return new ValueIterator<>(AbstractInternalDataContainer.this.iteratorIncludingExpired());
      }

      @Override
      public int size() {
         return AbstractInternalDataContainer.this.sizeIncludingExpired();
      }

      @Override
      public Spliterator<V> spliterator() {
         return Spliterators.spliterator(this, Spliterator.CONCURRENT);
      }
   }

   /**
    * @deprecated This is to removed when {@link #entrySet()} is removed
    */
   @Deprecated
   private static class ValueIterator<K, V> implements Iterator<V> {
      Iterator<InternalCacheEntry<K, V>> currentIterator;

      private ValueIterator(Iterator<InternalCacheEntry<K, V>> it) {
         currentIterator = it;
      }

      @Override
      public boolean hasNext() {
         return currentIterator.hasNext();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      @Override
      public V next() {
         return currentIterator.next().getValue();
      }
   }

   @Override
   public Set<InternalCacheEntry<K, V>> entrySet() {
      return new EntrySet();
   }

   /**
    * @deprecated this class is to be removed when {@link #entrySet()} is removed
    */
   @Deprecated
   private class ImmutableEntryIterator extends EntryIterator {
      ImmutableEntryIterator(Iterator<InternalCacheEntry<K, V>> it){
         super(it);
      }

      @Override
      public InternalCacheEntry<K, V> next() {
         return CoreImmutables.immutableInternalCacheEntry(super.next());
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Set
    *
    */
   private class EntrySet extends AbstractSet<InternalCacheEntry<K, V>> {

      @Override
      public boolean contains(Object o) {
         if (!(o instanceof Map.Entry)) {
            return false;
         }

         @SuppressWarnings("rawtypes")
         Map.Entry e = (Map.Entry) o;
         InternalCacheEntry ice = AbstractInternalDataContainer.this.get(e.getKey());
         if (ice == null) {
            return false;
         }
         return ice.getValue().equals(e.getValue());
      }

      @Override
      public Iterator<InternalCacheEntry<K, V>> iterator() {
         return new ImmutableEntryIterator(AbstractInternalDataContainer.this.iteratorIncludingExpired());
      }

      @Override
      public int size() {
         return AbstractInternalDataContainer.this.sizeIncludingExpired();
      }

      @Override
      public String toString() {
         return stream()
               .map(Object::toString)
               .collect(Collectors.joining(",", "[", "]"));
      }

      @Override
      public Spliterator<InternalCacheEntry<K, V>> spliterator() {
         return Spliterators.spliterator(this, Spliterator.DISTINCT | Spliterator.CONCURRENT);
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
      });
   }

   static <K, V> Caffeine<K, V> caffeineBuilder() {
      return (Caffeine<K, V>) Caffeine.newBuilder();
   }

   /**
    * Handles eviction logic. That is that it will order notifications, passivation and notify listeners as applicable
    * @param key key of the evicted entry
    * @param value evicted entry
    * @param orderer used to guarantee ordering between other operations. May be null when an operation is already ordered
    * @param passivator Passivates the entry to the store if necessary
    * @param evictionManager Handles additional eviction logic. May be null if eviction is also not required
    * @param <K> key type of the entry
    * @param <V> value type of the entry
    * @return stage that when complete all of the eviction logic is complete
    */
   public static <K, V> CompletionStage<Void> handleEviction(K key, InternalCacheEntry<K, V> value,
         NonBlockingOrderer orderer, PassivationManager passivator, EvictionManager<K, V> evictionManager,
         DataContainer<K, V> dataContainer) {
      CompletableFuture<OPERATION> future = new CompletableFuture<>();
      CompletionStage<OPERATION> ordererStage = null;
      if (orderer != null) {
         ordererStage = orderer.orderOn(key, future);
      }
      if (ordererStage != null) {
         // This code branch can only occur from 2 different outcomes
         // 1. Concurrent write operation (put, remove etc.)
         // 2. Activation where it has written to DataContainer but it was evicted before removing from store
         return ordererStage.thenCompose(operation -> {
            // If the entry was removed or was written to before we could get orderer than ignore passivation
            switch (operation) {
               case REMOVE:
                  // If it was a remove operation we always skip the eviction as the entry has been removed
                  return skipEviction(orderer, key, future, operation);
               case WRITE:
                  // During a write we skip the passivation/eviction if the entry is still in the container
                  // This means we were evicting but had another write concurrently - so the write wins
                  // If we are here it means this is after the eviction has completed - and thus if the
                  // entry is in the container it means the write put it there, so we skip eviction
                  if (dataContainer.containsKey(key)) {
                     return skipEviction(orderer, key, future, operation);
                  }
               default:
                  // This is a concurrent regular read - in which case we passivate just as normal
                  return handleNotificationAndOrderer(key, value, passivator.passivateAsync(value), orderer, evictionManager, future);
            }
         });
      }
      return handleNotificationAndOrderer(key, value, passivator.passivateAsync(value), orderer, evictionManager, future);
   }

   private static CompletionStage<Void> skipEviction(NonBlockingOrderer orderer, Object key,
         CompletableFuture<OPERATION> future, OPERATION op) {
      if (trace) {
         log.tracef("Skipping passivation for key %s due to %s", key, op);
      }
      orderer.completeOperation(key, future, OPERATION.READ);
      return CompletableFutures.completedNull();
   }

   private static <K, V> CompletionStage<Void> handleNotificationAndOrderer(K key, InternalCacheEntry<K, V> value,
         CompletionStage<Void> stage, NonBlockingOrderer orderer, EvictionManager<K, V> evictionManager,
         CompletableFuture<OPERATION> future) {
      if (evictionManager != null) {
         stage = stage.thenCompose(ignore -> evictionManager.onEntryEviction(Collections.singletonMap(key, value)));
      }
      if (orderer != null) {
         return stage.whenComplete((ignore, ignoreT) -> orderer.completeOperation(key, future, OPERATION.READ));
      }
      return stage;
   }

   final class DefaultEvictionListener {

      void onEntryChosenForEviction(K key, InternalCacheEntry<K, V> value) {
         handleEviction(key, value, orderer, passivator.running(), evictionManager, AbstractInternalDataContainer.this);
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
