package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.FlatMappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs flat map operation on a {@link IntStream}
 */
public class FlatMapIntOperation implements FlatMappingOperation<Integer, IntStream, Integer, IntStream> {
   private final IntFunction<? extends IntStream> function;

   public FlatMapIntOperation(IntFunction<? extends IntStream> function) {
      this.function = function;
   }

   @ProtoFactory
   FlatMapIntOperation(MarshallableObject<IntFunction<? extends IntStream>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<IntFunction<? extends IntStream>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.flatMap(function);
   }

   @Override
   public Stream<IntStream> map(IntStream intStream) {
      return intStream.mapToObj(function);
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input.concatMapStream(i -> function.apply(i).boxed());
   }
}
