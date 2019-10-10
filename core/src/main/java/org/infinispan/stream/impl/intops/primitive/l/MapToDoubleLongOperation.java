package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.Flowable;

/**
 * Performs map to double operation on a {@link LongStream}
 */
public class MapToDoubleLongOperation implements MappingOperation<Long, LongStream, Double, DoubleStream> {
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

   @Override
   public Flowable<Double> mapFlowable(Flowable<Long> input) {
      return input.map(function::applyAsDouble);
   }
}
