package org.infinispan.container.impl;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
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
import org.infinispan.factories.annotations.Stop;
import org.infinispan.marshall.core.WrappedByteArraySizeCalculator;
import org.reactivestreams.Publisher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;

import io.reactivex.rxjava3.core.Flowable;
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

   private final PeekableTouchableMap<K, V> entries;
   private final Cache<K, InternalCacheEntry<K, V>> evictionCache;

   public DefaultDataContainer(int concurrencyLevel) {
      // If no comparing implementations passed, could fallback on JDK CHM
      entries = new PeekableTouchableContainerMap<>(new ConcurrentHashMap<>(128));
      evictionCache = null;
   }

   protected DefaultDataContainer(int concurrencyLevel, long thresholdSize, boolean memoryBound) {
      DefaultEvictionListener evictionListener = new DefaultEvictionListener();
      Caffeine<K, InternalCacheEntry<K, V>> caffeine = caffeineBuilder();

      if (memoryBound) {
         CacheEntrySizeCalculator<K, V> calc = new CacheEntrySizeCalculator<>(new WrappedByteArraySizeCalculator<>(
               new PrimitiveEntrySizeCalculator()));
         caffeine.weigher((k, v) -> (int) calc.calculateSize(k, v)).maximumWeight(thresholdSize);
      } else {
         caffeine.maximumSize(thresholdSize);
      }
      evictionCache = applyListener(caffeine, evictionListener).build();
      entries = new PeekableTouchableCaffeineMap<>(evictionCache);
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
            .maximumWeight(thresholdSize), evictionListener)
            .build();

      entries = new PeekableTouchableCaffeineMap<>(evictionCache);
   }

   public static <K, V> DefaultDataContainer<K, V> boundedDataContainer(int concurrencyLevel, long maxEntries, boolean memoryBound) {
      return new DefaultDataContainer<>(concurrencyLevel, maxEntries, memoryBound);
   }

   public static <K, V> DefaultDataContainer<K, V> boundedDataContainer(int concurrencyLevel, long maxEntries,
                                                                        EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      return new DefaultDataContainer<>(concurrencyLevel, maxEntries, sizeCalculator);
   }

   public static <K, V> DefaultDataContainer<K, V> unBoundedDataContainer(int concurrencyLevel) {
      return new DefaultDataContainer<>(concurrencyLevel);
   }

   @Override
   protected PeekableTouchableMap<K, V> getMapForSegment(int segment) {
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

   @Stop
   @Override
   public void clear() {
      log.tracef("Clearing data container");
      entries.clear();
   }

   @Override
   public Publisher<InternalCacheEntry<K, V>> publisher(IntSet segments) {
      return Flowable.fromIterable(() -> iterator(segments));
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
   public long evictionSize() {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      return evict.weightedSize().orElse(entries.size());
   }

   @Override
   public void addSegments(IntSet segments) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void removeSegments(IntSet segments) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void cleanUp() {
      // Caffeine may not evict an entry right away if concurrent threads are writing, so this forces a cleanUp
      if (evictionCache != null) {
         evictionCache.cleanUp();
      }
   }


   @Override
   public void forEachSegment(ObjIntConsumer<PeekableTouchableMap<K, V>> segmentMapConsumer) {
      segmentMapConsumer.accept(entries, 0);
   }
}
