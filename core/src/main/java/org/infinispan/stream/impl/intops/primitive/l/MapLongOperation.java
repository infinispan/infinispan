package org.infinispan.stream.impl.intops.primitive.l;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

/**
 * Performs map operation on a {@link LongStream}
 */
public class MapLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream> {
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
}
