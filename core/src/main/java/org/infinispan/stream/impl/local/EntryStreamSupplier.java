package org.infinispan.stream.impl.local;

import static org.infinispan.commons.dataconversion.EncodingUtils.toStorage;

import java.util.BitSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Stream supplier that is to be used when the underlying stream is composed by {@link CacheEntry} instances.  This
 * supplier will do the proper filtering by key based on the CacheEntry key.
 */
public class EntryStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<K, V>, Stream<CacheEntry<K, V>>> {
   private static final Log log = LogFactory.getLog(EntryStreamSupplier.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final ConsistentHash hash;
   private final Supplier<Stream<CacheEntry<K, V>>> supplier;
   private final Wrapper wrapper = new ByteArrayWrapper();

   public EntryStreamSupplier(Cache<K, V> cache, ConsistentHash hash, Supplier<Stream<CacheEntry<K, V>>> supplier) {
      this.cache = cache;
      this.hash = hash;
      this.supplier = supplier;
   }

   @Override
   public Stream<CacheEntry<K, V>> buildStream(Set<Integer> segmentsToFilter, Set<?> keysToFilter) {
      Stream<CacheEntry<K, V>> stream;
      if (keysToFilter != null) {
         if (trace) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);

         Encoder encoder = advancedCache.getKeyEncoder();

         // Need to box the key to get the correct segment

         // We do type converter before getting CacheEntry, otherwise wrapper classes would have to both box and
         // unbox, this way we avoid multiple calls and just do the one.
         stream = keysToFilter.stream()
               .filter(Objects::nonNull)
               .map(o -> toStorage(o, encoder, wrapper))
               .map(advancedCache::getCacheEntry)
               .filter(e -> e != null);
      } else {
         stream = supplier.get();
      }
      if (segmentsToFilter != null && hash != null) {
         if (trace) {
            log.tracef("Applying segment filter %s", segmentsToFilter);
         }
         BitSet bitSet = new BitSet(hash.getNumSegments());
         segmentsToFilter.forEach(bitSet::set);
         stream = stream.filter(k -> bitSet.get(hash.getSegment(k.getKey())));
      }
      return stream;
   }

   @Override
   public CloseableIterator<CacheEntry<K, V>> removableIterator(CloseableIterator<CacheEntry<K, V>> realIterator) {
      return new RemovableCloseableIterator<>(realIterator, e -> cache.remove(e.getKey(), e.getValue()));
   }
}
