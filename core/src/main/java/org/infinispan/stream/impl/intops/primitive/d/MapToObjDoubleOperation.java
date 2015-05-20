package org.infinispan.stream.impl.intops.primitive.d;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.DoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Performs boxed operation on a {@link DoubleStream}
 * @param <R> the type of the output stream
 */
public class MapToObjDoubleOperation<R> implements IntermediateOperation<Double, DoubleStream, R, Stream<R>> {
   private final DoubleFunction<? extends R> function;

   public MapToObjDoubleOperation(DoubleFunction<? extends R> function) {
      this.function = function;
   }

   @Override
   public Stream<R> perform(DoubleStream stream) {
      return stream.mapToObj(function);
   }

   public DoubleFunction<? extends R> getFunction() {
      return function;
   }
}
