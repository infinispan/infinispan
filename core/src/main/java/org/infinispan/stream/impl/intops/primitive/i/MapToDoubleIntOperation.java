package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.Flowable;

/**
 * Performs map to double operation on a {@link IntStream}
 */
public class MapToDoubleIntOperation implements MappingOperation<Integer, IntStream, Double, DoubleStream> {
   private final IntToDoubleFunction function;

   public MapToDoubleIntOperation(IntToDoubleFunction function) {
      this.function = function;
   }

   @Override
   public DoubleStream perform(IntStream stream) {
      return stream.mapToDouble(function);
   }

   public IntToDoubleFunction getFunction() {
      return function;
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Integer> input) {
      return input.map(function::applyAsDouble);
   }
}
