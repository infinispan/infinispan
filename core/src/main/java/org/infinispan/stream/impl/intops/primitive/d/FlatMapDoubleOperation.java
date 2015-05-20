package org.infinispan.stream.impl.intops.primitive.d;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.DoubleFunction;
import java.util.stream.DoubleStream;

/**
 * Performs flat map operation on a {@link DoubleStream}
 */
public class FlatMapDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private final DoubleFunction<? extends DoubleStream> function;

   public FlatMapDoubleOperation(DoubleFunction<? extends DoubleStream> function) {
      this.function = function;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.flatMap(function);
   }

   public DoubleFunction<? extends DoubleStream> getFunction() {
      return function;
   }
}
