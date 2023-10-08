package org.infinispan.stream.impl.intops.object;

import java.util.stream.Stream;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs distinct operation on a regular {@link Stream}
 * @param <S> the type in the stream
 */
public class DistinctOperation<S> implements IntermediateOperation<S, Stream<S>, S, Stream<S>> {
   private static final DistinctOperation<?> OPERATION = new DistinctOperation<>();
   private DistinctOperation() { }

   @ProtoFactory
   public static <S> DistinctOperation<S> getInstance() {
      return (DistinctOperation<S>) OPERATION;
   }

   @Override
   public Stream<S> perform(Stream<S> stream) {
      return stream.distinct();
   }

   @Override
   public Flowable<S> mapFlowable(Flowable<S> input) {
      return input.distinct();
   }
}
