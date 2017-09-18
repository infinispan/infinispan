package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.FlatMappingOperation;

/**
 * Performs flat map operation on a {@link IntStream}
 */
public class FlatMapIntOperation implements FlatMappingOperation<Integer, IntStream, Integer, IntStream> {
   private final IntFunction<? extends IntStream> function;

   public FlatMapIntOperation(IntFunction<? extends IntStream> function) {
      this.function = function;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.flatMap(function);
   }

   public IntFunction<? extends IntStream> getFunction() {
      return function;
   }

   @Override
   public Stream<IntStream> map(IntStream intStream) {
      return intStream.mapToObj(function);
   }
}
