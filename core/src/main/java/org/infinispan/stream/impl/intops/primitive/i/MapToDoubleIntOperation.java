package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to double operation on a {@link IntStream}
 */
public class MapToDoubleIntOperation implements MappingOperation<Integer, IntStream, Double, DoubleStream> {
   private final IntToDoubleFunction function;

   public MapToDoubleIntOperation(IntToDoubleFunction function) {
      this.function = function;
   }

   @ProtoFactory
   MapToDoubleIntOperation(MarshallableObject<IntToDoubleFunction> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<IntToDoubleFunction> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public DoubleStream perform(IntStream stream) {
      return stream.mapToDouble(function);
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Integer> input) {
      return input.map(function::applyAsDouble);
   }
}
