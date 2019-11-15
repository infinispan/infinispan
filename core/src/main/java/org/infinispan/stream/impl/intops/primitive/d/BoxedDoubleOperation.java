package org.infinispan.stream.impl.intops.primitive.d;

import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.Flowable;

/**
 * Performs boxed operation on a {@link DoubleStream}
 */
public class BoxedDoubleOperation implements MappingOperation<Double, DoubleStream, Double, Stream<Double>> {
   private static final BoxedDoubleOperation OPERATION = new BoxedDoubleOperation();
   private BoxedDoubleOperation() { }

   public static BoxedDoubleOperation getInstance() {
      return OPERATION;
   }

   @Override
   public Stream<Double> perform(DoubleStream stream) {
      return stream.boxed();
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input;
   }
}
