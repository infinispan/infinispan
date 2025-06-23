package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to int operation on a {@link LongStream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_PRIMITIVE_LONG_MAP_TO_INT_OPERATION)
public class MapToIntLongOperation implements MappingOperation<Long, LongStream, Integer, IntStream> {
   private final LongToIntFunction function;

   public MapToIntLongOperation(LongToIntFunction function) {
      this.function = function;
   }

   @ProtoFactory
   MapToIntLongOperation(MarshallableObject<LongToIntFunction> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(1)
   MarshallableObject<LongToIntFunction> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public IntStream perform(LongStream stream) {
      return stream.mapToInt(function);
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Long> input) {
      return input.map(function::applyAsInt);
   }
}
