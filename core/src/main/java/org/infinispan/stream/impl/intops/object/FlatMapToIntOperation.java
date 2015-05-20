package org.infinispan.stream.impl.intops.object;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Performs flat map to int operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class FlatMapToIntOperation<I> implements IntermediateOperation<I, Stream<I>, Integer, IntStream> {
   private final Function<? super I, ? extends IntStream> function;

   public FlatMapToIntOperation(Function<? super I, ? extends IntStream> function) {
      this.function = function;
   }

   @Override
   public IntStream perform(Stream<I> stream) {
      return stream.flatMapToInt(function);
   }

   public Function<? super I, ? extends IntStream> getFunction() {
      return function;
   }
}
