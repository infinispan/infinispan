package org.infinispan.stream.impl.intops.primitive.i;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.IntToLongFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Performs map to long operation on a {@link IntStream}
 */
public class MapToLongIntOperation implements IntermediateOperation<Integer, IntStream, Long, LongStream> {
   private final IntToLongFunction function;

   public MapToLongIntOperation(IntToLongFunction function) {
      this.function = function;
   }

   @Override
   public LongStream perform(IntStream stream) {
      return stream.mapToLong(function);
   }

   public IntToLongFunction getFunction() {
      return function;
   }
}
