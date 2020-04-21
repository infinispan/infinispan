package org.infinispan.stream.impl.intops.primitive.d;

import java.util.stream.DoubleStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs skip operation on a {@link DoubleStream}
 */
public class SkipDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private final long n;

   public SkipDoubleOperation(long n) {
      this.n = n;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.skip(n);
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.skip(n);
   }
}
