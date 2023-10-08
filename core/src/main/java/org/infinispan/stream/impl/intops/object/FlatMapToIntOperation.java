package org.infinispan.stream.impl.intops.object;

import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.FlatMappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs flat map to int operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class FlatMapToIntOperation<I> implements FlatMappingOperation<I, Stream<I>, Integer, IntStream> {
   private final Function<? super I, ? extends IntStream> function;

   public FlatMapToIntOperation(Function<? super I, ? extends IntStream> function) {
      this.function = function;
   }

   @ProtoFactory
   FlatMapToIntOperation(MarshallableObject<Function<? super I, ? extends IntStream>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<Function<? super I, ? extends IntStream>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public IntStream perform(Stream<I> stream) {
      return stream.flatMapToInt(function);
   }

   @Override
   public Stream<IntStream> map(Stream<I> iStream) {
      return iStream.map(function);
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<I> input) {
      return input.concatMapStream(o -> function.apply(o).boxed());
   }
}
