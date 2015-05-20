package org.infinispan.stream.impl.intops.primitive.l;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Performs map to object operation on a {@link LongStream}
 */
public class MapToObjLongOperation<R> implements IntermediateOperation<Long, LongStream, R, Stream<R>> {
   private final LongFunction<? extends R> function;

   public MapToObjLongOperation(LongFunction<? extends R> function) {
      this.function = function;
   }

   @Override
   public Stream<R> perform(LongStream stream) {
      return stream.mapToObj(function);
   }

   public LongFunction<? extends R> getFunction() {
      return function;
   }
}
