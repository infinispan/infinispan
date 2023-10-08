package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map operation on a {@link IntStream}
 */
public class MapIntOperation implements MappingOperation<Integer, IntStream, Integer, IntStream> {
   private final IntUnaryOperator operator;

   public MapIntOperation(IntUnaryOperator operator) {
      this.operator = operator;
   }

   @ProtoFactory
   MapIntOperation(MarshallableObject<IntUnaryOperator> operator) {
      this.operator = MarshallableObject.unwrap(operator);
   }

   @ProtoField(1)
   MarshallableObject<IntUnaryOperator> getOperator() {
      return MarshallableObject.create(operator);
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.map(operator);
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input.map(operator::applyAsInt);
   }
}
