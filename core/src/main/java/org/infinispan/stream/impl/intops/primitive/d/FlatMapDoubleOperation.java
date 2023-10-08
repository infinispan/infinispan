package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.FlatMappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs flat map operation on a {@link DoubleStream}
 */
public class FlatMapDoubleOperation implements FlatMappingOperation<Double, DoubleStream, Double, DoubleStream> {
   private final DoubleFunction<? extends DoubleStream> function;

   public FlatMapDoubleOperation(DoubleFunction<? extends DoubleStream> function) {
      this.function = function;
   }

   @ProtoFactory
   FlatMapDoubleOperation(MarshallableObject<DoubleFunction<? extends DoubleStream>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(1)
   MarshallableObject<DoubleFunction<? extends DoubleStream>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.flatMap(function);
   }

   @Override
   public Stream<DoubleStream> map(DoubleStream doubleStream) {
      return doubleStream.mapToObj(function);
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.concatMapStream(d -> function.apply(d).boxed());
   }
}
