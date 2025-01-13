package org.infinispan.stream.impl.intops.object;

import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.FlatMappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs flat map to double operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
public class FlatMapToDoubleOperation<I> implements FlatMappingOperation<I, Stream<I>, Double, DoubleStream> {
   private final Function<? super I, ? extends DoubleStream> function;

   public FlatMapToDoubleOperation(Function<? super I, ? extends DoubleStream> function) {
      this.function = function;
   }

   @ProtoFactory
   FlatMapToDoubleOperation(MarshallableObject<Function<? super I, ? extends DoubleStream>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(number = 1)
   MarshallableObject<Function<? super I, ? extends DoubleStream>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public DoubleStream perform(Stream<I> stream) {
      return stream.flatMapToDouble(function);
   }

   @Override
   public Stream<DoubleStream> map(Stream<I> iStream) {
      return iStream.map(function);
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<I> input) {
      return input.concatMapStream(o -> function.apply(o).boxed());
   }
}
