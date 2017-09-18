package org.infinispan.stream.impl.intops.object;

import java.util.function.ToLongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.stream.impl.intops.MappingOperation;

/**
 * Performs map to long operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class MapToLongOperation<I> implements MappingOperation<I, Stream<I>, Long, LongStream> {
   private final ToLongFunction<? super I> function;

   public MapToLongOperation(ToLongFunction<? super I> function) {
      this.function = function;
   }

   @Override
   public LongStream perform(Stream<I> stream) {
      return stream.mapToLong(function);
   }

   public ToLongFunction<? super I> getFunction() {
      return function;
   }
}
