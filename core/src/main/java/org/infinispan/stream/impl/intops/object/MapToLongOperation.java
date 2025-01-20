package org.infinispan.stream.impl.intops.object;

import java.util.function.ToLongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to long operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class MapToLongOperation<I> implements MappingOperation<I, Stream<I>, Long, LongStream> {
   private final ToLongFunction<? super I> function;

   public MapToLongOperation(ToLongFunction<? super I> function) {
      this.function = function;
   }

   @ProtoFactory
   MapToLongOperation(MarshallableObject<ToLongFunction<? super I>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<ToLongFunction<? super I>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public LongStream perform(Stream<I> stream) {
      return stream.mapToLong(function);
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<I> input) {
      return input.map(function::applyAsLong);
   }
}
