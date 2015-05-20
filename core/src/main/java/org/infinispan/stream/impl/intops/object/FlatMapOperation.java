package org.infinispan.stream.impl.intops.object;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Performs flat map operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 * @param <O> the type of the output stream
 */
public class FlatMapOperation<I, O> implements IntermediateOperation<I, Stream<I>, O, Stream<O>> {
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
}
