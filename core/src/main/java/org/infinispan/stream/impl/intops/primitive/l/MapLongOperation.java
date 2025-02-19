package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map operation on a {@link LongStream}
 */
public class MapLongOperation implements MappingOperation<Long, LongStream, Long, LongStream> {
   private final LongUnaryOperator operator;

   public MapLongOperation(LongUnaryOperator operator) {
      this.operator = operator;
   }

   @ProtoFactory
   MapLongOperation(MarshallableObject<LongUnaryOperator> operator) {
      this.operator = MarshallableObject.unwrap(operator);
   }

   @ProtoField(1)
   MarshallableObject<LongUnaryOperator> getOperator() {
      return MarshallableObject.create(operator);
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.map(operator);
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.map(operator::applyAsLong);
   }
}
