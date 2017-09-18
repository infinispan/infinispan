package org.infinispan.stream.impl;

import java.util.Iterator;
import java.util.Set;

/**
 * Iterator response returned when an iterator batch is sent back which contains the iterator, if any segments
 * were suspected and if the iterator has returned all values (complete).
 * @author wburns
 * @since 9.0
 */
public interface IteratorResponse<V> {
   /**
    * The iterator containing the elements from the response
    * @return the iterator
    */
   Iterator<V> getIterator();

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
