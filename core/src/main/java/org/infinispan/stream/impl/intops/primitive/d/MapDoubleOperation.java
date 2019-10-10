package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleUnaryOperator;
import java.util.stream.DoubleStream;

import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.Flowable;

/**
 * Performs map operation on a {@link DoubleStream}
 */
public class MapDoubleOperation implements MappingOperation<Double, DoubleStream, Double, DoubleStream> {
   private final DoubleUnaryOperator operator;

   public MapDoubleOperation(DoubleUnaryOperator operator) {
      this.operator = operator;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.map(operator);
   }

   public DoubleUnaryOperator getOperator() {
      return operator;
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.map(operator::applyAsDouble);
   }
}
