package org.infinispan.stream.impl.intops.object;

import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.FlatMappingOperation;

/**
 * Performs flat map to int operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class FlatMapToIntOperation<I> implements FlatMappingOperation<I, Stream<I>, Integer, IntStream> {
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

   @Override
   public Stream<IntStream> map(Stream<I> iStream) {
      return iStream.map(function);
   }
}
