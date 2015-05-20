package org.infinispan.stream.impl.intops.primitive.i;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Performs map to object operation on a {@link IntStream}
 */
public class MapToObjIntOperation<R> implements IntermediateOperation<Integer, IntStream, R, Stream<R>> {
   private final IntFunction<? extends R> function;

   public MapToObjIntOperation(IntFunction<? extends R> function) {
      this.function = function;
   }

   @Override
   public Stream<R> perform(IntStream stream) {
      return stream.mapToObj(function);
   }

   public IntFunction<? extends R> getFunction() {
      return function;
   }
}
