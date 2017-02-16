package org.infinispan.stream.impl.intops.object;

import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

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
}
