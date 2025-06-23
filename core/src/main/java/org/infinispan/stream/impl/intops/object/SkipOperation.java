package org.infinispan.stream.impl.intops.object;

import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs skip operation on a regular {@link Stream}
 */
public class SkipOperation<S> implements IntermediateOperation<S, Stream<S>, S, Stream<S>> {
   private final long n;

   public SkipOperation(long n) {
      this.n = n;
   }

   @Override
   public Stream<S> perform(Stream<S> stream) {
      return stream.skip(n);
   }

   @Override
   public Flowable<S> mapFlowable(Flowable<S> input) {
      return input.skip(n);
   }
}
