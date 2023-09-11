package org.infinispan.multimap.impl;

import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Since 15.0
 */
public interface SortableBucket<V> {

   class SortOptions {
       public boolean alpha = false;
       public boolean asc = true;
       public long offset = 0;
       public long count = -1;
       public boolean skipSort;
   }

   Stream<MultimapObjectWrapper<V>> stream();

   List<ScoredValue<V>> sort(SortOptions options);

   default List<ScoredValue<V>> sort(Stream<ScoredValue<V>> scoredValueStream, SortOptions options) {
      if (!options.skipSort) {
         scoredValueStream = scoredValueStream.sorted(options.asc? Comparator.naturalOrder() : Comparator.reverseOrder());
      } else if (options.skipSort && !options.asc) {
         Deque<ScoredValue<V>> reverse = new ArrayDeque<>();
         scoredValueStream.forEach(v -> reverse.offerFirst(v));
         scoredValueStream = reverse.stream();
      }

      return scoredValueStream
            .skip(options.offset)
            .limit(options.count < 0 ? Long.MAX_VALUE : options.count)
            .collect(Collectors.toList());
   }
}
