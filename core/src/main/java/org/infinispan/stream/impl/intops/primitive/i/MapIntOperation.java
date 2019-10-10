package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.Flowable;

/**
 * Performs map operation on a {@link IntStream}
 */
public class MapIntOperation implements MappingOperation<Integer, IntStream, Integer, IntStream> {
   private final IntUnaryOperator operator;

   public MapIntOperation(IntUnaryOperator operator) {
      this.operator = operator;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.map(operator);
   }

   public IntUnaryOperator getOperator() {
      return operator;
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input.map(operator::applyAsInt);
   }
}
