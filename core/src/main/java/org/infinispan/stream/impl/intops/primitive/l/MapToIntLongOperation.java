package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.Flowable;

/**
 * Performs map to int operation on a {@link LongStream}
 */
public class MapToIntLongOperation implements MappingOperation<Long, LongStream, Integer, IntStream> {
   private final LongToIntFunction function;

   public MapToIntLongOperation(LongToIntFunction function) {
      this.function = function;
   }

   @Override
   public IntStream perform(LongStream stream) {
      return stream.mapToInt(function);
   }

   public LongToIntFunction getFunction() {
      return function;
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Long> input) {
      return input.map(function::applyAsInt);
   }
}
