package org.infinispan.stream.impl.intops.object;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.Stream;

/**
 * Performs distinct operation on a regular {@link Stream}
 * @param <S> the type in the stream
 */
public class DistinctOperation<S> implements IntermediateOperation<S, Stream<S>, S, Stream<S>> {
   private static final DistinctOperation<?> OPERATION = new DistinctOperation<>();
   private DistinctOperation() { }

   public static <S> DistinctOperation<S> getInstance() {
      return (DistinctOperation<S>) OPERATION;
   }

   @Override
   public Stream<S> perform(Stream<S> stream) {
      return stream.distinct();
   }
}
