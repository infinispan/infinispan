package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.util.LazyConcatIterator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * StreamSupplier that allows for creating streams where they utilize the {@link PersistenceManager} to publish entries
 * using segments if possible.
 * @author wburns
 * @since 9.4
 */
public class PersistenceEntryStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<K, V>, Stream<CacheEntry<K, V>>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final Cache<K, V> cache;
   private final InternalEntryFactory iceFactory;
   private final ToIntFunction<Object> toIntFunction;
   private final CacheStream<CacheEntry<K, V>> inMemoryStream;
   private final PersistenceManager persistenceManager;

   public PersistenceEntryStreamSupplier(Cache<K, V> cache, InternalEntryFactory iceFactory,
         ToIntFunction<Object> toIntFunction, CacheStream<CacheEntry<K, V>> inMemoryStream,
         PersistenceManager persistenceManager) {
      this.cache = cache;
      this.iceFactory = iceFactory;
      this.toIntFunction = toIntFunction;
      this.inMemoryStream = inMemoryStream;
      this.persistenceManager = persistenceManager;
   }

   @Override
   public Stream<CacheEntry<K, V>> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter, boolean parallel) {
      Stream<CacheEntry<K, V>> stream;
      if (keysToFilter != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);
         stream = keysToFilter.stream()
               .map(advancedCache::getCacheEntry)
               .filter(Objects::nonNull);
         if (segmentsToFilter != null && toIntFunction != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Applying segment filter %s", segmentsToFilter);
            }
            stream = stream.filter(k -> {
               K key = k.getKey();
               int segment = toIntFunction.applyAsInt(key);
               boolean isPresent = segmentsToFilter.contains(segment);
               if (log.isTraceEnabled())
                  log.tracef("Is key %s present in segment %d? %b", key, segment, isPresent);
               return isPresent;
            });
         }
      } else {
         Publisher<MarshallableEntry<K, V>> publisher;
         CacheStream<CacheEntry<K, V>> inMemoryStream = this.inMemoryStream;
         Set<K> seenKeys = new HashSet<>(2048);
         if (segmentsToFilter != null) {
            inMemoryStream = inMemoryStream.filterKeySegments(segmentsToFilter);
            publisher = persistenceManager.publishEntries(segmentsToFilter, k -> !seenKeys.contains(k), true, true,
                  PersistenceManager.AccessMode.BOTH);

         } else {
            publisher = persistenceManager.publishEntries(k -> !seenKeys.contains(k), true, true,
                  PersistenceManager.AccessMode.BOTH);
         }
         CloseableIterator<CacheEntry<K, V>> localIterator = new IteratorMapper<>(Closeables.iterator(inMemoryStream), e -> {
            seenKeys.add(e.getKey());
            return e;
         });
         Flowable<CacheEntry<K, V>> flowable = Flowable.fromPublisher(publisher)
               .map(me -> PersistenceUtil.convert(me, iceFactory));
         CloseableIterator<CacheEntry<K, V>> iterator = new LazyConcatIterator<>(localIterator,
               () -> Closeables.iterator(flowable, 128));

         Iterable<CacheEntry<K, V>> iterable = () -> iterator;
         // Make sure we close the iterator when the resulting stream is closed
         stream = StreamSupport.stream(iterable.spliterator(), parallel).onClose(iterator::close);
      }
      return stream;
   }
}
