package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleUnaryOperator;
import java.util.stream.DoubleStream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map operation on a {@link DoubleStream}
 */
public class MapDoubleOperation implements MappingOperation<Double, DoubleStream, Double, DoubleStream> {
   private final DoubleUnaryOperator operator;

   public MapDoubleOperation(DoubleUnaryOperator operator) {
      this.operator = operator;
   }

   @ProtoFactory
   MapDoubleOperation(MarshallableObject<DoubleUnaryOperator> operator) {
      this.operator = MarshallableObject.unwrap(operator);
   }

   @ProtoField(1)
   MarshallableObject<DoubleUnaryOperator> getOperator() {
      return MarshallableObject.create(operator);
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.map(operator);
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.map(operator::applyAsDouble);
   }
}
