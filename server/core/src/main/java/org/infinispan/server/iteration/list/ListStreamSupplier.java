package org.infinispan.server.iteration.list;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.stream.impl.local.AbstractLocalCacheStream;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Reads from a {@link Map} and builds a {@link Stream} of {@link CacheEntry} instances.
 * <p>
 * The entries generated in the stream are {@link ImmortalCacheEntry} instances, and don't have metadata.
 *
 * @since 15.0
 */
public class ListStreamSupplier implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<Object, Object>, Stream<CacheEntry<Object, Object>>> {

   private final List<Map.Entry<Object, Object>> source;

   public ListStreamSupplier(List<Map.Entry<Object, Object>> source) {
      this.source = source;
   }

   @Override
   public Stream<CacheEntry<Object, Object>> buildStream(IntSet ignore, Set valuesToFilter, boolean parallel) {
      if (source == null) return Stream.empty();

      Stream<Map.Entry<Object, Object>> stream = parallel ? source.parallelStream() : source.stream();
      return stream.map(this::convert);
   }

   private CacheEntry<Object, Object> convert(Map.Entry<Object, Object> entry) {
      return new ImmortalCacheEntry(entry.getKey(), entry.getValue());
   }
}
