package org.infinispan.stream.impl.intops.object;

import java.util.function.ToLongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to long operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_MAP_TO_LONG_OPERATION)
public class MapToLongOperation<I> implements MappingOperation<I, Stream<I>, Long, LongStream> {
   private final ToLongFunction<? super I> function;

   public MapToLongOperation(ToLongFunction<? super I> function) {
      this.function = function;
   }

   @ProtoFactory
   MapToLongOperation(MarshallableObject<ToLongFunction<? super I>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(1)
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
