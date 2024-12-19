package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to object operation on a {@link LongStream}
 */
public class MapToObjLongOperation<R> implements MappingOperation<Long, LongStream, R, Stream<R>> {
   private final LongFunction<? extends R> function;

   public MapToObjLongOperation(LongFunction<? extends R> function) {
      this.function = function;
   }

   @ProtoFactory
   MapToObjLongOperation(MarshallableObject<LongFunction<? extends R>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<LongFunction<? extends R>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public Stream<R> perform(LongStream stream) {
      return stream.mapToObj(function);
   }

   @Override
   public Flowable<R> mapFlowable(Flowable<Long> input) {
      return input.map(function::apply);
   }
}
