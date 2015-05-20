package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.PeekableMap;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Eviction;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.EvictionListener;
import org.infinispan.commons.util.concurrent.jdk8backported.EntrySizeCalculator;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MarshalledValueEntrySizeCalculator;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.TimeService;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;

/**
 * DefaultDataContainer is both eviction and non-eviction based data container.
 *
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @since 4.0
 */
@ThreadSafe
public class DefaultDataContainer<K, V> implements DataContainer<K, V> {

   private static final Log log = LogFactory.getLog(DefaultDataContainer.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentMap<K, InternalCacheEntry<K, V>> entries;
   protected InternalEntryFactory entryFactory;
   private EvictionManager evictionManager;
   private PassivationManager passivator;
   private ActivationManager activator;
   private PersistenceManager pm;
   private TimeService timeService;

   public DefaultDataContainer(int concurrencyLevel) {
      // If no comparing implementations passed, could fallback on JDK CHM
      entries = CollectionFactory.makeConcurrentParallelMap(128, concurrencyLevel);
   }

   public DefaultDataContainer(int concurrencyLevel,
         Equivalence<? super K> keyEq) {
      // If at least one comparing implementation give, use ComparingCHMv8
      entries = CollectionFactory.makeConcurrentParallelMap(128, concurrencyLevel, keyEq, AnyEquivalence.getInstance());
   }

   protected DefaultDataContainer(int concurrencyLevel, long thresholdSize,
         EvictionStrategy strategy, EvictionThreadPolicy policy,
         Equivalence<? super K> keyEquivalence, EvictionType thresholdPolicy) {
      DefaultEvictionListener evictionListener;
      // translate eviction policy and strategy
      switch (policy) {
         case PIGGYBACK:
         case DEFAULT:
            evictionListener = new DefaultEvictionListener();
            break;
         default:
            throw new IllegalArgumentException("No such eviction thread policy " + strategy);
      }

      Eviction eviction;
      switch (strategy) {
         case FIFO:
         case UNORDERED:
         case LRU:
            eviction = Eviction.LRU;
            break;
         case LIRS:
            eviction = Eviction.LIRS;
            if (thresholdPolicy == EvictionType.MEMORY) {
               throw new IllegalArgumentException("Memory based approximation eviction cannot be used with LIRS!");
            }
            break;
         default:
            throw new IllegalArgumentException("No such eviction strategy " + strategy);
      }
      EntrySizeCalculator<K, InternalCacheEntry<K, V>> sizeCalculator =
            thresholdPolicy == EvictionType.MEMORY ? new CacheEntrySizeCalculator<>(
                    new MarshalledValueEntrySizeCalculator()) : null;

      entries = new BoundedEquivalentConcurrentHashMapV8<>(thresholdSize, eviction, evictionListener, keyEquivalence,
              AnyEquivalence.getInstance(), sizeCalculator);
   }

   protected DefaultDataContainer(int concurrencyLevel, long thresholdSize,
                                  EvictionStrategy strategy, EvictionThreadPolicy policy,
                                  Equivalence<? super K> keyEquivalence,
                                  EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      DefaultEvictionListener evictionListener;
      // translate eviction policy and strategy
      switch (policy) {
         case PIGGYBACK:
         case DEFAULT:
            evictionListener = new DefaultEvictionListener();
            break;
         default:
            throw new IllegalArgumentException("No such eviction thread policy " + strategy);
      }

      EntrySizeCalculator<K, InternalCacheEntry<K, V>> calc = new CacheEntrySizeCalculator<>(sizeCalculator);

      entries = new BoundedEquivalentConcurrentHashMapV8<>(thresholdSize, Eviction.LRU, evictionListener, keyEquivalence,
              AnyEquivalence.getInstance(), calc);
   }

   @Inject
   public void initialize(EvictionManager evictionManager, PassivationManager passivator,
                          InternalEntryFactory entryFactory, ActivationManager activator, PersistenceManager clm, TimeService timeService) {
      this.evictionManager = evictionManager;
      this.passivator = passivator;
      this.entryFactory = entryFactory;
      this.activator = activator;
      this.pm = clm;
      this.timeService = timeService;
   }

   public static <K, V> DefaultDataContainer<K, V> boundedDataContainer(int concurrencyLevel, long maxEntries,
            EvictionStrategy strategy, EvictionThreadPolicy thredPolicy,
            Equivalence<? super K> keyEquivalence, EvictionType thresholdPolicy) {
      return new DefaultDataContainer<>(concurrencyLevel, maxEntries, strategy,
            thredPolicy, keyEquivalence, thresholdPolicy);
   }

   public static <K, V> DefaultDataContainer<K, V> boundedDataContainer(int concurrencyLevel, long maxEntries,
                                                                        EvictionStrategy strategy, EvictionThreadPolicy thredPolicy,
                                                                        Equivalence<? super K> keyEquivalence,
                                                                        EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      return new DefaultDataContainer<>(concurrencyLevel, maxEntries, strategy,
              thredPolicy, keyEquivalence, sizeCalculator);
   }

   public static <K, V> DefaultDataContainer<K, V> unBoundedDataContainer(int concurrencyLevel,
         Equivalence<? super K> keyEquivalence) {
      return new DefaultDataContainer<>(concurrencyLevel, keyEquivalence);
   }

   public static <K, V> DefaultDataContainer<K, V> unBoundedDataContainer(int concurrencyLevel) {
      return new DefaultDataContainer<>(concurrencyLevel);
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object key) {
      if (entries instanceof PeekableMap) {
         return ((PeekableMap<K, InternalCacheEntry<K, V>>)entries).peek(key);
      }
      return entries.get(key);
   }

   @Override
   public InternalCacheEntry<K, V> get(Object k) {
      InternalCacheEntry<K, V> e = entries.get(k);
      if (e != null && e.canExpire()) {
         long currentTimeMillis = timeService.wallClockTime();
         if (e.isExpired(currentTimeMillis)) {
            // We can only remove the same value - in case if a concurrent write updates it
            entries.remove(k, e);
            e = null;
         } else {
            e.touch(currentTimeMillis);
         }
      }
      return e;
   }

   @Override
   public void put(K k, V v, Metadata metadata) {
      boolean l1Entry = false;
      if (metadata instanceof L1Metadata) {
         metadata = ((L1Metadata) metadata).metadata();
         l1Entry = true;
      }
      InternalCacheEntry<K, V> e = entries.get(k);

      if (trace) {
         log.tracef("Creating new ICE for writing. Existing=%s, metadata=%s, new value=%s", e, metadata, v);
      }
      final InternalCacheEntry<K, V> copy;
      if (l1Entry) {
         copy = entryFactory.createL1(k, v, metadata);
      } else if (e != null) {
         copy = entryFactory.update(e, v, metadata);
      } else {
         // this is a brand-new entry
         copy = entryFactory.create(k, v, metadata);
      }

      if (trace)
         log.tracef("Store %s in container", e);

      entries.compute(copy.getKey(), (key, entry) -> {
         activator.onUpdate(key, entry == null);
         return copy;
      });
   }

   @Override
   public boolean containsKey(Object k) {
      InternalCacheEntry<K, V> ice = peek(k);
      if (ice != null && ice.canExpire() && ice.isExpired(timeService.wallClockTime())) {
         entries.remove(k);
         ice = null;
      }
      return ice != null;
   }

   @Override
   public InternalCacheEntry<K, V> remove(Object k) {
      final InternalCacheEntry<K,V>[] reference = new InternalCacheEntry[1];
      entries.compute((K) k, (key, entry) -> {
         activator.onRemove(key, entry == null);
         reference[0] = entry;
         return null;
      });
      InternalCacheEntry<K, V> e = reference[0];
      return e == null || (e.canExpire() && e.isExpired(timeService.wallClockTime())) ? null : e;
   }

   @Override
   public int size() {
      return entries.size();
   }

   @Override
   public void clear() {
      log.tracef("Clearing data container");
      entries.clear();
   }

   @Override
   public Set<K> keySet() {
      return Collections.unmodifiableSet(entries.keySet());
   }

   @Override
   public Collection<V> values() {
      return new Values();
   }

   @Override
   public Set<InternalCacheEntry<K, V>> entrySet() {
      return new EntrySet();
   }

   @Override
   public void purgeExpired() {
      long currentTimeMillis = timeService.wallClockTime();
      for (Iterator<InternalCacheEntry<K, V>> purgeCandidates = entries.values().iterator(); purgeCandidates.hasNext();) {
         InternalCacheEntry e = purgeCandidates.next();
         if (e.isExpired(currentTimeMillis)) {
            purgeCandidates.remove();
         }
      }
   }

   @Override
   public void evict(K key) {
      entries.computeIfPresent(key, (o, entry) -> {
         passivator.passivate(entry);
         return null;
      });
   }

   @Override
   public InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action) {
      return entries.compute(key, (k, oldEntry) -> {
         InternalCacheEntry<K, V> newEntry = action.compute(k, oldEntry, entryFactory);
         if (newEntry == oldEntry) {
            return oldEntry;
         } else if (newEntry == null) {
            activator.onRemove(k, false);
            return null;
         }
         activator.onUpdate(k, oldEntry == null);
         if (trace)
            log.tracef("Store %s in container", newEntry);
         return newEntry;
      });
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      return new EntryIterator(entries.values().iterator());
   }

   private final class DefaultEvictionListener implements EvictionListener<K, InternalCacheEntry<K, V>> {

      @Override
      public void onEntryEviction(Map<K, InternalCacheEntry<K, V>> evicted) {
         evictionManager.onEntryEviction(evicted);
      }

      @Override
      public void onEntryChosenForEviction(Entry<K, InternalCacheEntry<K, V>> entry) {
         passivator.passivate(entry.getValue());
      }

      @Override
      public void onEntryActivated(Object key) {
         activator.onUpdate(key, true);
      }

      @Override
      public void onEntryRemoved(Entry<K, InternalCacheEntry<K, V>> entry) {
         if (entry.getValue().isEvicted()) {
            onEntryChosenForEviction(entry);
         } else if (pm != null) {
            pm.deleteFromAllStores(entry.getKey(), BOTH);
         }
      }
   }

   private class ImmutableEntryIterator extends EntryIterator {
      ImmutableEntryIterator(Iterator<InternalCacheEntry<K, V>> it){
         super(it);
      }

      @Override
      public InternalCacheEntry<K, V> next() {
         return CoreImmutables.immutableInternalCacheEntry(super.next());
      }
   }

   public class EntryIterator implements Iterator<InternalCacheEntry<K, V>> {

      private final Iterator<InternalCacheEntry<K, V>> it;

      private InternalCacheEntry<K, V> next;

      EntryIterator(Iterator<InternalCacheEntry<K, V>> it){this.it=it;}

      private InternalCacheEntry<K, V> getNext() {
         while (it.hasNext()) {
            InternalCacheEntry<K, V> entry = it.next();
            long now;
            if (entry.canExpire() && entry.isExpired((now = timeService.wallClockTime()))) {
               InternalCacheEntry<K, V> computed = entries.compute(entry.getKey(), (k, oldEntry) -> {
                  if (oldEntry.isExpired(now)) {
                     return null;
                  }
                  return oldEntry;
               });
               if (computed != null) {
                  return computed;
               }
            } else {
               // Iterator doesn't count as a regular read, so don't touch entry
               return entry;
            }
         }
         return null;
      }

      @Override
      public InternalCacheEntry<K, V> next() {
         if (next == null) {
            next = getNext();
         }
         if (next == null) {
            throw new NoSuchElementException();
         }
         InternalCacheEntry<K, V> toReturn = next;
         next = null;
         return toReturn;
      }

      @Override
      public boolean hasNext() {
         if (next == null) {
            next = getNext();
         }
         return next != null;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
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
         InternalCacheEntry ice = entries.get(e.getKey());
         if (ice == null) {
            return false;
         }
         return ice.getValue().equals(e.getValue());
      }

      @Override
      public Iterator<InternalCacheEntry<K, V>> iterator() {
         return new ImmutableEntryIterator(entries.values().iterator());
      }

      @Override
      public int size() {
         return entries.size();
      }

      @Override
      public String toString() {
         return entries.toString();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Collection
    *
    */
   private class Values extends AbstractCollection<V> {
      @Override
      public Iterator<V> iterator() {
         return new ValueIterator(entries.values().iterator());
      }

      @Override
      public int size() {
         return entries.size();
      }
   }

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
   public void executeTask(final KeyFilter<? super K> filter, final BiConsumer<? super K, InternalCacheEntry<K, V>> action)
         throws InterruptedException {
      if (filter == null)
         throw new IllegalArgumentException("No filter specified");
      if (action == null)
         throw new IllegalArgumentException("No action specified");

      ParallelIterableMap<K, InternalCacheEntry<K, V>> map = (ParallelIterableMap<K, InternalCacheEntry<K, V>>) entries;
      map.forEach(32, (K key, InternalCacheEntry<K, V> value) -> {
         if (filter.accept(key)) {
            action.accept(key, value);
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }

   @Override
   public void executeTask(final KeyValueFilter<? super K, ? super V> filter, final BiConsumer<? super K, InternalCacheEntry<K, V>> action)
         throws InterruptedException {
      if (filter == null)
         throw new IllegalArgumentException("No filter specified");
      if (action == null)
         throw new IllegalArgumentException("No action specified");

      ParallelIterableMap<K, InternalCacheEntry<K, V>> map = (ParallelIterableMap<K, InternalCacheEntry<K, V>>) entries;
      map.forEach(32, (K key, InternalCacheEntry<K, V> value) -> {
         if (filter.accept(key, value.getValue(), value.getMetadata())) {
            action.accept(key, value);
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }
}
