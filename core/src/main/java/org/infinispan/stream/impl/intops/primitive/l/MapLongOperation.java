package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.Flowable;

/**
 * Performs map operation on a {@link LongStream}
 */
public class MapLongOperation implements MappingOperation<Long, LongStream, Long, LongStream> {
   private final LongUnaryOperator operator;

   public MapLongOperation(LongUnaryOperator operator) {
      this.operator = operator;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.map(operator);
   }

   public LongUnaryOperator getOperator() {
      return operator;
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.map(operator::applyAsLong);
   }
}
