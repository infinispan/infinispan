package org.infinispan.stream.impl.intops.primitive.i;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Performs as long operation on a {@link IntStream}
 */
public class AsLongIntOperation implements IntermediateOperation<Integer, IntStream, Long, LongStream> {
   private static final AsLongIntOperation OPERATION = new AsLongIntOperation();
   private AsLongIntOperation() { }

   public static AsLongIntOperation getInstance() {
      return OPERATION;
   }

   @Override
   public LongStream perform(IntStream stream) {
      return stream.asLongStream();
   }
}
