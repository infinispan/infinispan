package org.infinispan.stream.impl.intops.primitive.d;

import java.util.stream.DoubleStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

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
}
