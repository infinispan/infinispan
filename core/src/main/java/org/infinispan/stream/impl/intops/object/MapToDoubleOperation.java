package org.infinispan.stream.impl.intops.object;

import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to double operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_MAP_TO_DOUBLE_OPERATION)
public class MapToDoubleOperation<I> implements MappingOperation<I, Stream<I>, Double, DoubleStream> {
   private final ToDoubleFunction<? super I> function;

   public MapToDoubleOperation(ToDoubleFunction<? super I> function) {
      this.function = function;
   }

   @ProtoFactory
   MapToDoubleOperation(MarshallableObject<ToDoubleFunction<? super I>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(1)
   MarshallableObject<ToDoubleFunction<? super I>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public DoubleStream perform(Stream<I> stream) {
      return stream.mapToDouble(function);
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<I> input) {
      return input.map(function::applyAsDouble);
   }
}
