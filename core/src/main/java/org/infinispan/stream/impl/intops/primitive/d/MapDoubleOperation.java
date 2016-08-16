package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleUnaryOperator;
import java.util.stream.DoubleStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Performs map operation on a {@link DoubleStream}
 */
public class MapDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
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
}
