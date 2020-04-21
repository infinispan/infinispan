package org.infinispan.stream.impl.intops.primitive.i;

import java.util.stream.IntStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs skip operation on a {@link IntStream}
 */
public class SkipIntOperation implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {
   private final long n;

   public SkipIntOperation(long n) {
      this.n = n;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.skip(n);
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input.skip(n);
   }
}
