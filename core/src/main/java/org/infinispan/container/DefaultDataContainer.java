package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.commons.util.concurrent.ParallelIterableMap.KeyValueAction;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.Eviction;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.EvictionListener;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

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
   private final DefaultEvictionListener evictionListener;
   private final ExtendedMap<K, V> extendedMap;
   protected InternalEntryFactory entryFactory;
   private EvictionManager evictionManager;
   private PassivationManager passivator;
   private ActivationManager activator;
   private PersistenceManager pm;
   private TimeService timeService;

   public DefaultDataContainer(int concurrencyLevel) {
      // If no comparing implementations passed, could fallback on JDK CHM
      entries = CollectionFactory.makeConcurrentParallelMap(128, concurrencyLevel);
      evictionListener = null;
      extendedMap = new EquivalentConcurrentExtendedMap();
   }

   public DefaultDataContainer(int concurrencyLevel,
         Equivalence<? super K> keyEq) {
      // If at least one comparing implementation give, use ComparingCHMv8
      entries = CollectionFactory.makeConcurrentParallelMap(128, concurrencyLevel, keyEq, AnyEquivalence.getInstance());
      evictionListener = null;
      extendedMap = new EquivalentConcurrentExtendedMap();
   }

   protected DefaultDataContainer(int concurrencyLevel, int maxEntries,
         EvictionStrategy strategy, EvictionThreadPolicy policy,
         Equivalence<? super K> keyEquivalence) {
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
            break;
         default:
            throw new IllegalArgumentException("No such eviction strategy " + strategy);
      }

      entries = new BoundedConcurrentHashMap<K, InternalCacheEntry<K, V>>(maxEntries, concurrencyLevel, eviction, evictionListener,
                                                                          keyEquivalence, AnyEquivalence.getInstance());
      extendedMap = new BoundedConcurrentExtendedMap();
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

   public static <K, V> DataContainer<K, V> boundedDataContainer(int concurrencyLevel, int maxEntries,
            EvictionStrategy strategy, EvictionThreadPolicy policy,
            Equivalence<? super K> keyEquivalence) {
      return new DefaultDataContainer(concurrencyLevel, maxEntries, strategy,
            policy, keyEquivalence);
   }

   public static <K, V> DataContainer<K, V> unBoundedDataContainer(int concurrencyLevel,
         Equivalence<? super K> keyEquivalence) {
      return new DefaultDataContainer(concurrencyLevel, keyEquivalence);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel) {
      return new DefaultDataContainer(concurrencyLevel);
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object key) {
      return entries.get(key);
   }

   @Override
   public InternalCacheEntry<K, V> get(Object k) {
      InternalCacheEntry<K, V> e = peek(k);
      if (e != null && e.canExpire()) {
         long currentTimeMillis = timeService.wallClockTime();
         if (e.isExpired(currentTimeMillis)) {
            entries.remove(k);
            e = null;
         } else {
            e.touch(currentTimeMillis);
         }
      }
      return e;
   }

   @Override
   public void put(K k, V v, Metadata metadata) {
      InternalCacheEntry<K, V> e = entries.get(k);

      if (trace) {
         log.tracef("Creating new ICE for writing. Existing=%s, metadata=%s, new value=%s", e, metadata, v);
      }
      if (e != null) {
         e = entryFactory.update(e, v, metadata);
      } else {
         // this is a brand-new entry
         e = entryFactory.create(k, v, metadata);
      }

      if (trace)
         log.tracef("Store %s in container", e);

      extendedMap.putAndActivate(e);
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
      InternalCacheEntry<K, V> e = extendedMap.removeAndActivate(k);
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
      extendedMap.evict(key);
   }

   @Override
   public InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action) {
      return extendedMap.compute(key, action);
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
      public void onEntryChosenForEviction(InternalCacheEntry entry) {
         passivator.passivate(entry);
      }

      @Override
      public void onEntryActivated(Object key) {
         activator.onUpdate(key, true);
      }

      @Override
      public void onEntryRemoved(Object key) {
         if (pm != null)
            pm.deleteFromAllStores(key, BOTH);
      }
   }

   private static class ImmutableEntryIterator<K, V> extends EntryIterator<K, V> {
      ImmutableEntryIterator(Iterator<InternalCacheEntry<K, V>> it){
         super(it);
      }

      @Override
      public InternalCacheEntry<K, V> next() {
         return CoreImmutables.immutableInternalCacheEntry(super.next());
      }
   }

   public static class EntryIterator<K, V> implements Iterator<InternalCacheEntry<K, V>> {

      private final Iterator<InternalCacheEntry<K, V>> it;

      EntryIterator(Iterator<InternalCacheEntry<K, V>> it){this.it=it;}

      @Override
      public InternalCacheEntry<K, V> next() {
         return it.next();
      }

      @Override
      public boolean hasNext() {
         return it.hasNext();
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
   public void executeTask(final KeyFilter<? super K> filter, final KeyValueAction<? super K, InternalCacheEntry<K, V>> action)
         throws InterruptedException {
      if (filter == null)
         throw new IllegalArgumentException("No filter specified");
      if (action == null)
         throw new IllegalArgumentException("No action specified");

      ParallelIterableMap<K, InternalCacheEntry<K, V>> map = (ParallelIterableMap<K, InternalCacheEntry<K, V>>) entries;
      map.forEach(32, new KeyValueAction<K, InternalCacheEntry<K, V>>() {
         @Override
         public void apply(K key, InternalCacheEntry<K, V> value) {
            if (filter.accept(key)) {
               action.apply(key, value);
            }
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }

   @Override
   public void executeTask(final KeyValueFilter<? super K, ? super V> filter, final KeyValueAction<? super K, InternalCacheEntry<K, V>> action)
         throws InterruptedException {
      if (filter == null)
         throw new IllegalArgumentException("No filter specified");
      if (action == null)
         throw new IllegalArgumentException("No action specified");

      ParallelIterableMap<K, InternalCacheEntry<K, V>> map = (ParallelIterableMap<K, InternalCacheEntry<K, V>>) entries;
      map.forEach(32, new KeyValueAction<K, InternalCacheEntry<K, V>>() {
         @Override
         public void apply(K key, InternalCacheEntry<K, V> value) {
            if (filter.accept(key, value.getValue(), value.getMetadata())) {
               action.apply(key, value);
            }
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }

   /**
    * Atomic logic to activate/passivate entries. This is dependent of the {@code ConcurrentMap} implementation.
    */
   private static interface ExtendedMap<K, V> {
      void evict(K key);

      InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action);

      void putAndActivate(InternalCacheEntry<K, V> newEntry);

      InternalCacheEntry<K, V> removeAndActivate(Object key);
   }

   private class EquivalentConcurrentExtendedMap implements ExtendedMap<K, V> {
      @Override
      public void evict(K key) {
         ((EquivalentConcurrentHashMapV8<K, InternalCacheEntry<K, V>>) entries)
               .computeIfPresent(key, new EquivalentConcurrentHashMapV8.BiFun<K, InternalCacheEntry<K, V>, InternalCacheEntry<K, V>>() {
                  @Override
                  public InternalCacheEntry<K, V> apply(K o, InternalCacheEntry<K, V> entry) {
                     passivator.passivate(entry);
                     return null;
                  }
               });
      }

      @Override
      public InternalCacheEntry<K, V> compute(K key, final ComputeAction<K, V> action) {
         return ((EquivalentConcurrentHashMapV8<K, InternalCacheEntry<K, V>>) entries)
               .compute(key, new EquivalentConcurrentHashMapV8.BiFun<K, InternalCacheEntry<K, V>, InternalCacheEntry<K, V>>() {
                  @Override
                  public InternalCacheEntry<K, V> apply(K key, InternalCacheEntry<K, V> oldEntry) {
                     InternalCacheEntry<K, V> newEntry = action.compute(key, oldEntry, entryFactory);
                     if (newEntry == oldEntry) {
                        return oldEntry;
                     } else if (newEntry == null) {
                        activator.onRemove(key, false);
                        return null;
                     }
                     activator.onUpdate(key, oldEntry == null);
                     if (trace)
                        log.tracef("Store %s in container", newEntry);
                     return newEntry;
                  }
               });
      }

      @Override
      public void putAndActivate(final InternalCacheEntry<K, V> newEntry) {
         ((EquivalentConcurrentHashMapV8<K, InternalCacheEntry<K, V>>) entries)
               .compute(newEntry.getKey(), new EquivalentConcurrentHashMapV8.BiFun<K, InternalCacheEntry<K, V>, InternalCacheEntry<K, V>>() {
                  @Override
                  public InternalCacheEntry<K, V> apply(K key, InternalCacheEntry<K, V> entry) {
                     activator.onUpdate(key, entry == null);
                     return newEntry;
                  }
               });
      }

      @Override
      public InternalCacheEntry<K, V> removeAndActivate(Object key) {
         final AtomicReference<InternalCacheEntry<K,V>> reference = new AtomicReference<>(null);
         ((EquivalentConcurrentHashMapV8<Object, InternalCacheEntry<K, V>>) entries)
               .compute(key, new EquivalentConcurrentHashMapV8.BiFun<Object, InternalCacheEntry<K, V>, InternalCacheEntry<K, V>>() {
                  @Override
                  public InternalCacheEntry<K, V> apply(Object key, InternalCacheEntry<K, V> entry) {
                     activator.onRemove(key, entry == null);
                     reference.set(entry);
                     return null;
                  }
               });
         return reference.get();
      }
   }

   private class BoundedConcurrentExtendedMap implements ExtendedMap<K, V> {
      @Override
      public void evict(K key) {
         ((BoundedConcurrentHashMap<Object, InternalCacheEntry<K, V>>) entries).evict(key);
      }

      @Override
      public InternalCacheEntry<K, V> compute(K key, final ComputeAction<K, V> action) {
         final BoundedConcurrentHashMap<K, InternalCacheEntry<K, V>> boundedMap =
               ((BoundedConcurrentHashMap<K, InternalCacheEntry<K, V>>) entries);
         boundedMap.lock(key);
         try {
            InternalCacheEntry<K, V> oldEntry = boundedMap.get(key);
            InternalCacheEntry<K, V> newEntry = action.compute(key, oldEntry, entryFactory);
            if (oldEntry == newEntry) {
               return newEntry;
            } else if (newEntry == null) {
               activator.onRemove(key, false);
               boundedMap.remove(key);
               return null;
            }
            if (trace)
               log.tracef("Store %s in container", newEntry);
            //put already activate the entry if it is new.
            boundedMap.put(key, newEntry);
            return newEntry;
         } finally {
            boundedMap.unlock(key);
         }
      }

      @Override
      public void putAndActivate(InternalCacheEntry<K, V> newEntry) {
         //put already activate the entry if it is new.
         entries.put(newEntry.getKey(), newEntry);
      }

      @Override
      public InternalCacheEntry<K, V> removeAndActivate(Object key) {
         final BoundedConcurrentHashMap<Object, InternalCacheEntry<K, V>> boundedMap =
               ((BoundedConcurrentHashMap<Object, InternalCacheEntry<K, V>>) entries);
         boundedMap.lock(key);
         try {
            InternalCacheEntry<K, V> oldEntry = boundedMap.remove(key);
            activator.onRemove(key, oldEntry == null);
            return oldEntry;
         } finally {
            boundedMap.unlock(key);
         }
      }
   }
}
