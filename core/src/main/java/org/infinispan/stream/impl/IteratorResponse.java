package org.infinispan.stream.impl;

import java.util.Set;
import java.util.Spliterator;

/**
 * Iterator response returned when an iterator batch is sent back which contains the iterator, if any segments
 * were suspected and if the iterator has returned all values (complete).
 * @author wburns
 * @since 9.0
 */
public interface IteratorResponse<V> {
   /**
    * The spliterator containing the elements from the response. This spliterator is guaranteed to have a known
    * exact size when invoking {@link Spliterator#getExactSizeIfKnown()}.
    * @return the spliterator
    */
   Spliterator<V> getSpliterator();

   /**
    * Whether the iterator is the end or if more requests are needed
    * @return if no more elements are available
    */
   boolean isComplete();

   /**
    * The segments that were lost during the iteration process
    * @return the segments that need to be re-queried
    */
   Set<Integer> getSuspectedSegments();
}
