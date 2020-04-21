package org.infinispan.stream.impl.intops.primitive.l;

import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs skip operation on a {@link LongStream}
 */
public class SkipLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private final long n;

   public SkipLongOperation(long n) {
      this.n = n;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.skip(n);
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.skip(n);
   }
}
