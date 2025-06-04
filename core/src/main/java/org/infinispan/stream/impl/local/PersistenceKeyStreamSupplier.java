package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
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
import org.infinispan.context.Flag;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.LazyConcatIterator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * StreamSupplier that allows for creating streams where they utilize the {@link PersistenceManager} to publish keys
 * using segments if possible.
 * @author wburns
 * @since 9.4
 */
public class PersistenceKeyStreamSupplier<K> implements AbstractLocalCacheStream.StreamSupplier<K, Stream<K>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final Cache<K, ?> cache;
   private final ToIntFunction<Object> toIntFunction;
   private final CacheStream<K> inMemoryStream;
   private final PersistenceManager persistenceManager;

   public PersistenceKeyStreamSupplier(Cache<K, ?> cache, ToIntFunction<Object> toIntFunction,
         CacheStream<K> inMemoryStream, PersistenceManager persistenceManager) {
      this.cache = cache;
      this.toIntFunction = toIntFunction;
      this.inMemoryStream = inMemoryStream;
      this.persistenceManager = persistenceManager;
   }

   @Override
   public Stream<K> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter, boolean parallel) {
      Stream<K> stream;
      if (keysToFilter != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, ?> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);
         stream = (Stream<K>) (parallel ? keysToFilter.parallelStream() : keysToFilter.stream())
               .filter(advancedCache::containsKey);
         if (segmentsToFilter != null && toIntFunction != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Applying segment filter %s", segmentsToFilter);
            }
            stream = stream.filter(k -> {
               int segment = toIntFunction.applyAsInt(k);
               boolean isPresent = segmentsToFilter.contains(segment);
               if (log.isTraceEnabled())
                  log.tracef("Is key %s present in segment %d? %b", k, segment, isPresent);
               return isPresent;
            });
         }
      } else {
         Publisher<K> publisher;
         CacheStream<K> inMemoryStream = this.inMemoryStream;
         Set<K> seenKeys = new HashSet<>(2048);
         if (segmentsToFilter != null) {
            inMemoryStream = inMemoryStream.filterKeySegments(segmentsToFilter);
            publisher = persistenceManager.publishKeys(segmentsToFilter, k -> !seenKeys.contains(k),
                  PersistenceManager.AccessMode.BOTH);

         } else {
            publisher = persistenceManager.publishKeys(k -> !seenKeys.contains(k), PersistenceManager.AccessMode.BOTH);
         }
         CloseableIterator<K> localIterator = new IteratorMapper<>(Closeables.iterator(inMemoryStream), k -> {
            seenKeys.add(k);
            return k;
         });
         Flowable<K> flowable = Flowable.fromPublisher(publisher);
         CloseableIterator<K> iterator = new LazyConcatIterator<>(localIterator,
               () -> Closeables.iterator(flowable, 128));

         Iterable<K> iterable = () -> iterator;
         // Make sure we close the iterator when the resulting stream is closed
         stream = StreamSupport.stream(iterable.spliterator(), parallel).onClose(iterator::close);
      }
      return stream;
   }
}
