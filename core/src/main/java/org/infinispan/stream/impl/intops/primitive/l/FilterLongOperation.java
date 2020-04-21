package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongPredicate;
import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs filter operation on a {@link LongStream}
 */
public class FilterLongOperation<S> implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private final LongPredicate predicate;

   public FilterLongOperation(LongPredicate predicate) {
      this.predicate = predicate;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.filter(predicate);
   }

   public LongPredicate getPredicate() {
      return predicate;
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.filter(predicate::test);
   }
}
