package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.stream.impl.RemovableCloseableIterator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * Created by wburns on 10/9/15.
 */
public class KeyStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<K> {
   private static final Log log = LogFactory.getLog(KeyStreamSupplier.class);

   private final Cache<K, V> cache;
   private final ConsistentHash hash;
   private final Supplier<Stream<K>> supplier;

   public KeyStreamSupplier(Cache<K, V> cache, ConsistentHash hash, Supplier<Stream<K>> supplier) {
      this.cache = cache;
      this.hash = hash;
      this.supplier = supplier;
   }

   @Override
   public Stream<K> buildStream(Set<Integer> segmentsToFilter, Set<?> keysToFilter) {
      Stream<K> stream;
      if (keysToFilter != null) {
         log.tracef("Applying key filtering %s", keysToFilter);
         stream = (Stream<K>) keysToFilter.stream().filter(k -> cache.containsKey(k));
      } else {
         stream = supplier.get();
      }
      if (segmentsToFilter != null && hash != null) {
         log.tracef("Applying segment filter %s", segmentsToFilter);
         stream = stream.filter(k -> segmentsToFilter.contains(hash.getSegment(k)));
      }
      return stream;
   }

   @Override
   public CloseableIterator<K> removableIterator(CloseableIterator<K> realIterator) {
      return new RemovableCloseableIterator<>(realIterator, cache, Function.identity());
   }
}
