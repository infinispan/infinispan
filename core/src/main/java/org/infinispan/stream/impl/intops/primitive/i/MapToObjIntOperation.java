package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to object operation on a {@link IntStream}
 */
public class MapToObjIntOperation<R> implements MappingOperation<Integer, IntStream, R, Stream<R>> {
   private final IntFunction<? extends R> function;

   public MapToObjIntOperation(IntFunction<? extends R> function) {
      this.function = function;
   }

   @ProtoFactory
   MapToObjIntOperation(MarshallableObject<IntFunction<? extends R>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<IntFunction<? extends R>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public Stream<R> perform(IntStream stream) {
      return stream.mapToObj(function);
   }

   @Override
   public Flowable<R> mapFlowable(Flowable<Integer> input) {
      return input.map(function::apply);
   }
}
