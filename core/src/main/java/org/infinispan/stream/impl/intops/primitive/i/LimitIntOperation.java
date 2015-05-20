package org.infinispan.stream.impl.intops.primitive.i;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.IntStream;

/**
 * Performs limit operation on a {@link IntStream}
 */
public class LimitIntOperation implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {
   private final long limit;

   public LimitIntOperation(long limit) {
      if (limit <= 0) {
         throw new IllegalArgumentException("Limit must be greater than 0");
      }
      this.limit = limit;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.limit(limit);
   }

   public long getLimit() {
      return limit;
   }
}
