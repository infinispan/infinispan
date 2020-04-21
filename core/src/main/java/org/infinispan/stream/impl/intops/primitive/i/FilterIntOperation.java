package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs filter operation on a {@link IntStream}
 */
public class FilterIntOperation<S> implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {
   private final IntPredicate predicate;

   public FilterIntOperation(IntPredicate predicate) {
      this.predicate = predicate;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.filter(predicate);
   }

   public IntPredicate getPredicate() {
      return predicate;
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input.filter(predicate::test);
   }
}
