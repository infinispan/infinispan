package org.infinispan.stream.impl.intops.object;

import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.FlatMappingOperation;

/**
 * Performs flat map operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 * @param <O> the type of the output stream
 */
public class FlatMapOperation<I, O> implements FlatMappingOperation<I, Stream<I>, O, Stream<O>> {
   private final Function<? super I, ? extends Stream<? extends O>> function;

   public FlatMapOperation(Function<? super I, ? extends Stream<? extends O>> function) {
      this.function = function;
   }

   @Override
   public Stream<O> perform(Stream<I> stream) {
      return stream.flatMap(function);
   }

   public Function<? super I, ? extends Stream<? extends O>> getFunction() {
      return function;
   }

   @Override
   public Stream<Stream<O>> map(Stream<I> iStream) {
      // Have to cast to make generics happy
      return iStream.map((Function<I, Stream<O>>) function);
   }
}
