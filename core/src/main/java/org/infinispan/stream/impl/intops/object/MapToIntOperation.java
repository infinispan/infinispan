package org.infinispan.stream.impl.intops.object;

import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to int operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class MapToIntOperation<I> implements MappingOperation<I, Stream<I>, Integer, IntStream> {
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

   @Override
   public Flowable<Integer> mapFlowable(Flowable<I> input) {
      return input.map(function::applyAsInt);
   }
}
