package org.infinispan.stream.impl.intops.object;

import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to int operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class MapToIntOperation<I> implements MappingOperation<I, Stream<I>, Integer, IntStream> {
   private final ToIntFunction<? super I> function;

   public MapToIntOperation(ToIntFunction<? super I> function) {
      this.function = function;
   }

   @ProtoFactory
   MapToIntOperation(MarshallableObject<ToIntFunction<? super I>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<ToIntFunction<? super I>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public IntStream perform(Stream<I> stream) {
      return stream.mapToInt(function);
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<I> input) {
      return input.map(function::applyAsInt);
   }
}
