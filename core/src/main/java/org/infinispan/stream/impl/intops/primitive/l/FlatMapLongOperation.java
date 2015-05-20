package org.infinispan.stream.impl.intops.primitive.l;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.LongFunction;
import java.util.stream.LongStream;

/**
 * Performs flat map operation on a {@link LongStream}
 */
public class FlatMapLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private final LongFunction<? extends LongStream> function;

   public FlatMapLongOperation(LongFunction<? extends LongStream> function) {
      this.function = function;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.flatMap(function);
   }

   public LongFunction<? extends LongStream> getFunction() {
      return function;
   }
}
