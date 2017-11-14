package org.infinispan.container;

import static org.infinispan.commons.util.Util.toStr;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.EntrySizeCalculator;
import org.infinispan.commons.util.EvictionListener;
import org.infinispan.commons.util.PeekableMap;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionType;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.marshall.core.WrappedByteArraySizeCalculator;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;

import net.jcip.annotations.ThreadSafe;

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
   private final Cache<K, InternalCacheEntry<K, V>> evictionCache;
   protected InternalEntryFactory entryFactory;
   private EvictionManager evictionManager;
   private PassivationManager passivator;
   private ActivationManager activator;
   private PersistenceManager pm;
   private TimeService timeService;
   private CacheNotifier cacheNotifier;
   private ExpirationManager<K, V> expirationManager;

   public DefaultDataContainer(int concurrencyLevel) {
      // If no comparing implementations passed, could fallback on JDK CHM
      entries = CollectionFactory.makeConcurrentParallelMap(128, concurrencyLevel);
      evictionCache = null;
   }

   private static <K, V> Caffeine<K, V> caffeineBuilder() {
      return (Caffeine<K, V>) Caffeine.newBuilder();
   }

   protected DefaultDataContainer(int concurrencyLevel, long thresholdSize, EvictionType thresholdPolicy) {
      DefaultEvictionListener evictionListener = new DefaultEvictionListener();
      Caffeine<K, InternalCacheEntry<K, V>> caffeine = caffeineBuilder();

      switch (thresholdPolicy) {
         case MEMORY:
            CacheEntrySizeCalculator<K, V> calc = new CacheEntrySizeCalculator<>(new WrappedByteArraySizeCalculator<>(
                  new PrimitiveEntrySizeCalculator()));
            caffeine.weigher((k, v) -> (int) calc.calculateSize(k, v)).maximumWeight(thresholdSize);
            break;
         case COUNT:
            caffeine.maximumSize(thresholdSize);
            break;
         default:
            throw new UnsupportedOperationException("Policy not supported: " + thresholdPolicy);
      }
      evictionCache = applyListener(caffeine, evictionListener).build();
      entries = evictionCache.asMap();
   }

   private Caffeine<K, InternalCacheEntry<K, V>> applyListener(Caffeine<K, InternalCacheEntry<K, V>> caffeine, DefaultEvictionListener listener) {
      return caffeine.executor(new WithinThreadExecutor()).removalListener((k, v, c) -> {
         switch (c) {
            case SIZE:
               listener.onEntryEviction(Collections.singletonMap(k, v));
               break;
            case EXPLICIT:
               listener.onEntryRemoved(new ImmortalCacheEntry(k, v));
               break;
            case REPLACED:
               listener.onEntryActivated(k);
               break;
         }
      }).writer(new CacheWriter<K, InternalCacheEntry<K, V>>() {
         @Override
         public void write(K key, InternalCacheEntry<K, V> value) {

         }

         @Override
         public void delete(K key, InternalCacheEntry<K, V> value, RemovalCause cause) {
            if (cause == RemovalCause.SIZE) {
               listener.onEntryChosenForEviction(new ImmortalCacheEntry(key, value));
            }
         }
      });
   }

   /**
    * Method invoked when memory policy is used
    * @param concurrencyLevel
    * @param thresholdSize
    * @param sizeCalculator
    */
   protected DefaultDataContainer(int concurrencyLevel, long thresholdSize,
                                  EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      DefaultEvictionListener evictionListener = new DefaultEvictionListener();

      EntrySizeCalculator<K, InternalCacheEntry<K, V>> calc = new CacheEntrySizeCalculator<>(sizeCalculator);

      evictionCache = applyListener(Caffeine.newBuilder()
            .weigher((K k, InternalCacheEntry<K, V> v) -> (int) calc.calculateSize(k, v))
            .maximumWeight(thresholdSize), evictionListener)
            .build();

      entries = evictionCache.asMap();
   }

   @Inject
   public void initialize(EvictionManager evictionManager, PassivationManager passivator,
                          InternalEntryFactory entryFactory, ActivationManager activator, PersistenceManager clm,
                          TimeService timeService, CacheNotifier cacheNotifier, ExpirationManager<K, V> expirationManager) {
      this.evictionManager = evictionManager;
      this.passivator = passivator;
      this.entryFactory = entryFactory;
      this.activator = activator;
      this.pm = clm;
      this.timeService = timeService;
      this.cacheNotifier = cacheNotifier;
      this.expirationManager = expirationManager;
   }

   public static <K, V> DefaultDataContainer<K, V> boundedDataContainer(int concurrencyLevel, long maxEntries,
            EvictionType thresholdPolicy) {
      return new DefaultDataContainer<>(concurrencyLevel, maxEntries, thresholdPolicy);
   }

   public static <K, V> DefaultDataContainer<K, V> boundedDataContainer(int concurrencyLevel, long maxEntries,
                                                                        EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      return new DefaultDataContainer<>(concurrencyLevel, maxEntries, sizeCalculator);
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
            expirationManager.handleInMemoryExpiration(e, currentTimeMillis);
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
         log.tracef("Creating new ICE for writing. Existing=%s, metadata=%s, new value=%s", e, metadata, toStr(v));
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
         log.tracef("Store %s in container", copy);

      entries.compute(copy.getKey(), (key, entry) -> {
         activator.onUpdate(key, entry == null);
         return copy;
      });
   }

   @Override
   public boolean containsKey(Object k) {
      InternalCacheEntry<K, V> ice = peek(k);
      if (ice != null && ice.canExpire()) {
         long currentTimeMillis = timeService.wallClockTime();
         if (ice.isExpired(currentTimeMillis)) {
            expirationManager.handleInMemoryExpiration(ice, currentTimeMillis);
            ice = null;
         }
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
      if (trace) {
         log.tracef("Removed %s from container", e);
      }
      return e == null || (e.canExpire() && e.isExpired(timeService.wallClockTime())) ? null : e;
   }

   private Policy.Eviction<K, InternalCacheEntry<K, V>> eviction() {
      if (evictionCache != null) {
         Optional<Policy.Eviction<K, InternalCacheEntry<K, V>>> eviction = evictionCache.policy().eviction();
         if (eviction.isPresent()) {
            return eviction.get();
         }
      }
      throw new UnsupportedOperationException();
   }

   @Override
   public long capacity() {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      return evict.getMaximum();
   }

   @Override
   public void resize(long newSize) {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      evict.setMaximum(newSize);
   }

   @Override
   public int size() {
      int size = 0;
      // We have to loop through to make sure to remove expired entries
      for (Iterator<InternalCacheEntry<K, V>> iter = iterator(); iter.hasNext(); ) {
         iter.next();
         if (++size == Integer.MAX_VALUE) return Integer.MAX_VALUE;
      }
      return size;
   }

   @Override
   public int sizeIncludingExpired() {
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
      return new EntryIterator(entries.values().iterator(), false);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      return new EntryIterator(entries.values().iterator(), true);
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
      }
   }

   private class ImmutableEntryIterator extends EntryIterator {
      ImmutableEntryIterator(Iterator<InternalCacheEntry<K, V>> it){
         super(it, false);
      }

      @Override
      public InternalCacheEntry<K, V> next() {
         return CoreImmutables.immutableInternalCacheEntry(super.next());
      }
   }

   public class EntryIterator implements Iterator<InternalCacheEntry<K, V>> {

      private final Iterator<InternalCacheEntry<K, V>> it;
      private final boolean includeExpired;

      private InternalCacheEntry<K, V> next;

      EntryIterator(Iterator<InternalCacheEntry<K, V>> it, boolean includeExpired){
         this.it=it;
         this.includeExpired = includeExpired;
      }

      private InternalCacheEntry<K, V> getNext() {
         boolean initializedTime = false;
         long now = 0;
         while (it.hasNext()) {
            InternalCacheEntry<K, V> entry = it.next();
            if (includeExpired || !entry.canExpire()) {
               log.tracef("Return next entry %s", entry);
               return entry;
            } else {
               if (!initializedTime) {
                  now = timeService.wallClockTime();
                  initializedTime = true;
               }
               if (!entry.isExpired(now)) {
                  log.tracef("Return next entry %s", entry);
                  return entry;
               } else {
                  log.tracef("%s is expired", entry);
               }
            }
         }
         log.tracef("Return next null");
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

      @Override
      public Spliterator<InternalCacheEntry<K, V>> spliterator() {
         return Spliterators.spliterator(this, Spliterator.DISTINCT | Spliterator.CONCURRENT);
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

      @Override
      public Spliterator<V> spliterator() {
         return Spliterators.spliterator(this, Spliterator.CONCURRENT);
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

      long now = timeService.wallClockTime();
      entries.forEach((K key, InternalCacheEntry<K, V> value) -> {
         if (filter.accept(key) && !value.isExpired(now)) {
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

      long now = timeService.wallClockTime();
      entries.forEach((K key, InternalCacheEntry<K, V> value) -> {
         if (filter.accept(key, value.getValue(), value.getMetadata()) && !value.isExpired(now)) {
            action.accept(key, value);
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }
}
