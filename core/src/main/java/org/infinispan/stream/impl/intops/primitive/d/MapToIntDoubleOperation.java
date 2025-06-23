package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleToIntFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to int operation on a {@link DoubleStream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_PRIMITIVE_DOUBLE_MAP_TO_INT_OPERATION)
public class MapToIntDoubleOperation implements MappingOperation<Double, DoubleStream, Integer, IntStream> {
   private final DoubleToIntFunction function;

   public MapToIntDoubleOperation(DoubleToIntFunction function) {
      this.function = function;
   }

   @ProtoFactory
   MapToIntDoubleOperation(MarshallableObject<DoubleToIntFunction> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(1)
   MarshallableObject<DoubleToIntFunction> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public IntStream perform(DoubleStream stream) {
      return stream.mapToInt(function);
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Double> input) {
      return input.map(function::applyAsInt);
   }
}
