package org.infinispan.stream.impl.intops.primitive.i;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.IntStream;

/**
 * Performs sorted operation on a {@link IntStream}
 */
public class SortedIntOperation implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {
   private static final SortedIntOperation OPERATION = new SortedIntOperation();
   private SortedIntOperation() { }

   public static SortedIntOperation getInstance() {
      return OPERATION;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.sorted();
   }
}
