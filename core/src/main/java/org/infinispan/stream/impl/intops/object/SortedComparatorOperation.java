package org.infinispan.stream.impl.intops.object;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Performs sorted operation with a comparator on a regular {@link Stream}
 */
public class SortedComparatorOperation<S> implements IntermediateOperation<S, Stream<S>, S, Stream<S>> {
   private final Comparator<? super S> comparator;

   public SortedComparatorOperation(Comparator<? super S> comparator) {
      this.comparator = comparator;
   }

   @Override
   public Stream<S> perform(Stream<S> stream) {
      return stream.sorted(comparator);
   }

   public Comparator<? super S> getComparator() {
      return comparator;
   }
}
