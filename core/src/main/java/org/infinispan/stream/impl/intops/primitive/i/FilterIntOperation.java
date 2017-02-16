package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

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
}
