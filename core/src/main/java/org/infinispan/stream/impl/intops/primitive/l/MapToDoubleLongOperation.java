package org.infinispan.stream.impl.intops.primitive.l;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.LongToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 * Performs map to double operation on a {@link LongStream}
 */
public class MapToDoubleLongOperation implements IntermediateOperation<Long, LongStream, Double, DoubleStream> {
   private final LongToDoubleFunction function;

   public MapToDoubleLongOperation(LongToDoubleFunction function) {
      this.function = function;
   }

   @Override
   public DoubleStream perform(LongStream stream) {
      return stream.mapToDouble(function);
   }

   public LongToDoubleFunction getFunction() {
      return function;
   }
}
