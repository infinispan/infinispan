package org.infinispan.stream.impl.intops.primitive.d;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Performs boxed operation on a {@link DoubleStream}
 */
public class BoxedDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, Stream<Double>> {
   private static final BoxedDoubleOperation OPERATION = new BoxedDoubleOperation();
   private BoxedDoubleOperation() { }

   public static BoxedDoubleOperation getInstance() {
      return OPERATION;
   }

   @Override
   public Stream<Double> perform(DoubleStream stream) {
      return stream.boxed();
   }
}
