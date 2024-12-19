package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntToLongFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to long operation on a {@link IntStream}
 */
public class MapToLongIntOperation implements MappingOperation<Integer, IntStream, Long, LongStream> {
   private final IntToLongFunction function;

   public MapToLongIntOperation(IntToLongFunction function) {
      this.function = function;
   }

   @ProtoFactory
   MapToLongIntOperation(MarshallableObject<IntToLongFunction> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<IntToLongFunction> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public LongStream perform(IntStream stream) {
      return stream.mapToLong(function);
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Integer> input) {
      return input.map(function::applyAsLong);
   }
}
