package org.infinispan.stream.impl.intops.object;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Performs map to double operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class MapToDoubleOperation<I> implements IntermediateOperation<I, Stream<I>, Double, DoubleStream> {
   private final ToDoubleFunction<? super I> function;

   public MapToDoubleOperation(ToDoubleFunction<? super I> function) {
      this.function = function;
   }

   @Override
   public DoubleStream perform(Stream<I> stream) {
      return stream.mapToDouble(function);
   }

   public ToDoubleFunction<? super I> getFunction() {
      return function;
   }
}
