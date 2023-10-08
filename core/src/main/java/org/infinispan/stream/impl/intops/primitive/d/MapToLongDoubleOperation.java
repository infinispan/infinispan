package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleToLongFunction;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to long operation on a {@link DoubleStream}
 */
public class MapToLongDoubleOperation implements MappingOperation<Double, DoubleStream, Long, LongStream> {
   private final DoubleToLongFunction function;

   public MapToLongDoubleOperation(DoubleToLongFunction function) {
      this.function = function;
   }

   @ProtoFactory
   MapToLongDoubleOperation(MarshallableObject<DoubleToLongFunction> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(1)
   MarshallableObject<DoubleToLongFunction> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public LongStream perform(DoubleStream stream) {
      return stream.mapToLong(function);
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Double> input) {
      return input.map(function::applyAsLong);
   }
}
