package org.infinispan.stream.impl.intops.object;

import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.FlatMappingOperation;

/**
 * Performs flat map to long operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class FlatMapToLongOperation<I> implements FlatMappingOperation<I, Stream<I>, Long, LongStream> {
   private final Function<? super I, ? extends LongStream> function;

   public FlatMapToLongOperation(Function<? super I, ? extends LongStream> function) {
      this.function = function;
   }

   @Override
   public LongStream perform(Stream<I> stream) {
      return stream.flatMapToLong(function);
   }

   public Function<? super I, ? extends LongStream> getFunction() {
      return function;
   }

   @Override
   public Stream<LongStream> map(Stream<I> iStream) {
      return iStream.map(function);
   }
}
