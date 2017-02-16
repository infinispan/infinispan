package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntFunction;
import java.util.stream.IntStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Performs flat map operation on a {@link IntStream}
 */
public class FlatMapIntOperation implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {
   private final IntFunction<? extends IntStream> function;

   public FlatMapIntOperation(IntFunction<? extends IntStream> function) {
      this.function = function;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.flatMap(function);
   }

   public IntFunction<? extends IntStream> getFunction() {
      return function;
   }
}
