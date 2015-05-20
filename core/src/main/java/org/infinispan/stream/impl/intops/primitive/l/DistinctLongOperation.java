package org.infinispan.stream.impl.intops.primitive.l;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.LongStream;

/**
 * Performs distinct operation on a {@link LongStream}
 */
public class DistinctLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private static final DistinctLongOperation OPERATION = new DistinctLongOperation();
   private DistinctLongOperation() { }

   public static DistinctLongOperation getInstance() {
      return OPERATION;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.distinct();
   }
}
