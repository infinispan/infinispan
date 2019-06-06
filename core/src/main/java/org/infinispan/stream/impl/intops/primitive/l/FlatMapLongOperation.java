package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.stream.impl.intops.FlatMappingOperation;

import io.reactivex.Flowable;

/**
 * Performs flat map operation on a {@link LongStream}
 */
public class FlatMapLongOperation implements FlatMappingOperation<Long, LongStream, Long, LongStream> {
   private final LongFunction<? extends LongStream> function;

   public FlatMapLongOperation(LongFunction<? extends LongStream> function) {
      this.function = function;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.flatMap(function);
   }

   public LongFunction<? extends LongStream> getFunction() {
      return function;
   }

   @Override
   public Stream<LongStream> map(LongStream longStream) {
      return longStream.mapToObj(function);
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.flatMap(l -> RxJavaInterop.fromStream(function.apply(l).boxed()));
   }
}
