package org.infinispan.container.impl;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.EntrySizeCalculator;
import org.infinispan.commons.util.FilterIterator;
import org.infinispan.commons.util.FilterSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.eviction.EvictionType;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.marshall.core.WrappedByteArraySizeCalculator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;

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
public class DefaultDataContainer<K, V> extends AbstractInternalDataContainer<K, V> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final ConcurrentMap<K, InternalCacheEntry<K, V>> entries;
   private final Cache<K, InternalCacheEntry<K, V>> evictionCache;

   public DefaultDataContainer(int concurrencyLevel) {
      // If no comparing implementations passed, could fallback on JDK CHM
      entries = new ConcurrentHashMap<>(128);
      evictionCache = null;
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
      evictionCache = applyListener(caffeine, evictionListener, null).build();
      entries = evictionCache.asMap();
   }

   /**
    * Method invoked when memory policy is used. This calculator only calculates the given key and value.
    * @param concurrencyLevel
    * @param thresholdSize
    * @param sizeCalculator
    */
   protected DefaultDataContainer(int concurrencyLevel, long thresholdSize,
                                  EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      this(thresholdSize, new CacheEntrySizeCalculator<>(sizeCalculator));
   }

   /**
    * Constructor that allows user to provide a size calculator that also handles the cache entry and metadata.
    * @param thresholdSize
    * @param sizeCalculator
    */
   protected DefaultDataContainer(long thresholdSize,
         EntrySizeCalculator<? super K, ? super InternalCacheEntry<K, V>> sizeCalculator) {
      DefaultEvictionListener evictionListener = new DefaultEvictionListener();

      evictionCache = applyListener(Caffeine.newBuilder()
            .weigher((K k, InternalCacheEntry<K, V> v) -> (int) sizeCalculator.calculateSize(k, v))
            .maximumWeight(thresholdSize), evictionListener, null)
            .build();

      entries = evictionCache.asMap();
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
   protected ConcurrentMap<K, InternalCacheEntry<K, V>> getMapForSegment(int segment) {
      return entries;
   }

   @Override
   protected int getSegmentForKey(Object key) {
      // We always map to same map, so no reason to waste finding out segment
      return -1;
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
   public int sizeIncludingExpired() {
      return entries.size();
   }

   @Override
   public void clear(IntSet segments) {
      Iterator<InternalCacheEntry<K, V>> iter = iteratorIncludingExpired(segments);
      while (iter.hasNext()) {
         iter.next();
         iter.remove();
      }
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
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      return new EntryIterator(entries.values().iterator());
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator(IntSet segments) {
      return new FilterIterator<>(iterator(), ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator() {
      return filterExpiredEntries(spliteratorIncludingExpired());
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator(IntSet segments) {
      return new FilterSpliterator<>(spliterator(),
            ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired() {
      // Technically this spliterator is distinct, but it won't be set - we assume that is okay for now
      return entries.values().spliterator();
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments) {
      return new FilterSpliterator<>(spliteratorIncludingExpired(),
            ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      return entries.values().iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments) {
      return new FilterIterator<>(iteratorIncludingExpired(),
            ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public void forEachIncludingExpired(ObjIntConsumer<? super InternalCacheEntry<K, V>> action) {
      iteratorIncludingExpired().forEachRemaining(ice -> action.accept(ice, keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public long evictionSize() {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      return evict.weightedSize().orElse(entries.size());
   }

   @Override
   public void addSegments(IntSet segments) {
      // Don't have to do anything here
   }

   @Override
   public void removeSegments(IntSet segments) {
      InternalDataContainerAdapter.removeSegmentEntries(this, segments, listeners, keyPartitioner);
   }

   @Override
   public void executeTask(final KeyFilter<? super K> filter, final BiConsumer<? super K, InternalCacheEntry<K, V>> action)
         throws InterruptedException {
      if (filter == null)
         throw new IllegalArgumentException("No filter specified");
      if (action == null)
         throw new IllegalArgumentException("No action specified");

      forEach(e -> {
         if (filter.accept(e.getKey())) {
            action.accept(e.getKey(), e);
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
      forEach(e -> {
         if (filter.accept(e.getKey(), e.getValue(), e.getMetadata())) {
            action.accept(e.getKey(), e);
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if(Thread.currentThread().isInterrupted()){
         throw new InterruptedException();
      }
   }
}
