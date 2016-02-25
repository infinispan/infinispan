package org.infinispan.stream.impl.local;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.stream.impl.RemovableCloseableIterator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Stream supplier that is to be used when the underlying stream is composed by {@link CacheEntry} instances.  This
 * supplier will do the proper filtering by key based on the CacheEntry key.
 */
public class EntryStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<K, V>> {
   private static final Log log = LogFactory.getLog(EntryStreamSupplier.class);

   private final Cache<K, V> cache;
   private final ConsistentHash hash;
   private final Supplier<Stream<CacheEntry<K, V>>> supplier;

   public EntryStreamSupplier(Cache<K, V> cache, ConsistentHash hash, Supplier<Stream<CacheEntry<K, V>>> supplier) {
      this.cache = cache;
      this.hash = hash;
      this.supplier = supplier;
   }

   @Override
   public Stream<CacheEntry<K, V>> buildStream(Set<Integer> segmentsToFilter, Set<?> keysToFilter) {
      Stream<CacheEntry<K, V>> stream;
      if (keysToFilter != null) {
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
         log.tracef("Applying key filtering %s", keysToFilter);
         stream = keysToFilter.stream().map(advancedCache::getCacheEntry).filter(e -> e != null);
      } else {
         stream = supplier.get();
      }
      if (segmentsToFilter != null && hash != null) {
         log.tracef("Applying segment filter %s", segmentsToFilter);
         stream = stream.filter(k -> segmentsToFilter.contains(hash.getSegment(k.getKey())));
      }
      return stream;
   }

   @Override
   public CloseableIterator<CacheEntry<K, V>> removableIterator(CloseableIterator<CacheEntry<K, V>> realIterator) {
      return new RemovableCloseableIterator<>(realIterator, cache, CacheEntry::getKey);
   }
}
