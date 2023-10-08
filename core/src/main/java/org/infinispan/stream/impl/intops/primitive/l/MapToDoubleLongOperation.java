package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to double operation on a {@link LongStream}
 */
public class MapToDoubleLongOperation implements MappingOperation<Long, LongStream, Double, DoubleStream> {
   private final LongToDoubleFunction function;

   public MapToDoubleLongOperation(LongToDoubleFunction function) {
      this.function = function;
   }

   @ProtoFactory
   MapToDoubleLongOperation(MarshallableObject<LongToDoubleFunction> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<LongToDoubleFunction> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public DoubleStream perform(LongStream stream) {
      return stream.mapToDouble(function);
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Long> input) {
      return input.map(function::applyAsDouble);
   }
}
