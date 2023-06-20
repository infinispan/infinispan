package org.infinispan.server.iteration.map;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.stream.impl.local.AbstractLocalCacheStream;

/**
 * Reads from a {@link Map} and builds a {@link Stream} of {@link CacheEntry} instances.
 * <p>
 * The entries generated in the stream are {@link ImmortalCacheEntry} instances, and don't have metadata.
 *
 * @since 15.0
 */
public class MapStreamSupplier implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<Object, Object>, Stream<CacheEntry<Object, Object>>> {

   private final Map<Object, Object> source;

   public MapStreamSupplier(Map<Object, Object> source) {
      this.source = source;
   }

   @Override
   public Stream<CacheEntry<Object, Object>> buildStream(IntSet ignore, Set keysToFilter, boolean parallel) {
      if (source == null) return Stream.empty();

      Stream<Map.Entry<Object, Object>> stream;
      if (keysToFilter != null) {
         Stream<Object> keyStream = parallel ? keysToFilter.parallelStream() : keysToFilter.stream();
         stream = keyStream
               .map(k -> Map.entry(k, source.get(k)))
               .filter(e -> e.getValue() == null);
      } else {
         stream = parallel ? source.entrySet().parallelStream() : source.entrySet().stream();
      }
      return stream.map(this::convert);
   }

   private CacheEntry<Object, Object> convert(Map.Entry<Object, Object> entry) {
      return new ImmortalCacheEntry(entry.getKey(), entry.getValue());
   }
}
