package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.stream.impl.intops.FlatMappingOperation;

import io.reactivex.Flowable;

/**
 * Performs flat map operation on a {@link DoubleStream}
 */
public class FlatMapDoubleOperation implements FlatMappingOperation<Double, DoubleStream, Double, DoubleStream> {
   private final DoubleFunction<? extends DoubleStream> function;

   public FlatMapDoubleOperation(DoubleFunction<? extends DoubleStream> function) {
      this.function = function;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.flatMap(function);
   }

   public DoubleFunction<? extends DoubleStream> getFunction() {
      return function;
   }

   @Override
   public Stream<DoubleStream> map(DoubleStream doubleStream) {
      return doubleStream.mapToObj(function);
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.flatMap(d -> RxJavaInterop.fromStream(function.apply(d).boxed()));
   }
}
