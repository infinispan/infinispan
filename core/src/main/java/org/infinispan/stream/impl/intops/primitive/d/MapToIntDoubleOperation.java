package org.infinispan.stream.impl.intops.primitive.d;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.DoubleToIntFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

/**
 * Performs map to int operation on a {@link DoubleStream}
 */
public class MapToIntDoubleOperation implements IntermediateOperation<Double, DoubleStream, Integer, IntStream> {
   private final DoubleToIntFunction function;

   public MapToIntDoubleOperation(DoubleToIntFunction function) {
      this.function = function;
   }

   @Override
   public IntStream perform(DoubleStream stream) {
      return stream.mapToInt(function);
   }

   public DoubleToIntFunction getFunction() {
      return function;
   }
}
