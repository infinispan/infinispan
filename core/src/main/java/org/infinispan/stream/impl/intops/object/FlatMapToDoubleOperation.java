package org.infinispan.stream.impl.intops.object;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Performs flat map to double operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class FlatMapToDoubleOperation<I> implements IntermediateOperation<I, Stream<I>, Double, DoubleStream> {
   private final Function<? super I, ? extends DoubleStream> function;

   public FlatMapToDoubleOperation(Function<? super I, ? extends DoubleStream> function) {
      this.function = function;
   }

   @Override
   public DoubleStream perform(Stream<I> stream) {
      return stream.flatMapToDouble(function);
   }

   public Function<? super I, ? extends DoubleStream> getFunction() {
      return function;
   }
}