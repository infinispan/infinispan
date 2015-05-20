package org.infinispan.stream.impl.intops.object;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Performs map to int operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class MapToIntOperation<I> implements IntermediateOperation<I, Stream<I>, Integer, IntStream> {
   private final ToIntFunction<? super I> function;

   public MapToIntOperation(ToIntFunction<? super I> function) {
      this.function = function;
   }

   @Override
   public IntStream perform(Stream<I> stream) {
      return stream.mapToInt(function);
   }

   public ToIntFunction<? super I> getFunction() {
      return function;
   }
}
