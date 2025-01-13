package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.FlatMappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs flat map operation on a {@link LongStream}
 */
public class FlatMapLongOperation implements FlatMappingOperation<Long, LongStream, Long, LongStream> {
   private final LongFunction<? extends LongStream> function;

   public FlatMapLongOperation(LongFunction<? extends LongStream> function) {
      this.function = function;
   }

   @ProtoFactory
   FlatMapLongOperation(MarshallableObject<LongFunction<? extends LongStream>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<LongFunction<? extends LongStream>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.flatMap(function);
   }

   @Override
   public Stream<LongStream> map(LongStream longStream) {
      return longStream.mapToObj(function);
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.concatMapStream(l -> function.apply(l).boxed());
   }
}
