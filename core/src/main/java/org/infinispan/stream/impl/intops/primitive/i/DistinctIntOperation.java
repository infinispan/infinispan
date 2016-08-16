package org.infinispan.stream.impl.intops.primitive.i;

import java.util.stream.IntStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Performs distinct operation on a {@link IntStream}
 */
public class DistinctIntOperation implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {
   private static final DistinctIntOperation OPERATION = new DistinctIntOperation();
   private DistinctIntOperation() { }

   public static DistinctIntOperation getInstance() {
      return OPERATION;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.distinct();
   }
}
