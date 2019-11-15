package org.infinispan.stream.impl.intops.object;

import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.stream.impl.intops.FlatMappingOperation;

import io.reactivex.Flowable;

/**
 * Performs flat map operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 * @param <O> the type of the output stream
 */
public class FlatMapOperation<I, O> implements FlatMappingOperation<I, Stream<I>, O, Stream<O>> {
   private final Function<? super I, ? extends Stream<? extends O>> function;

   public FlatMapOperation(Function<? super I, ? extends Stream<? extends O>> function) {
      this.function = function;
   }

   @Override
   public Stream<O> perform(Stream<I> stream) {
      return stream.flatMap(function);
   }

   public Function<? super I, ? extends Stream<? extends O>> getFunction() {
      return function;
   }

   @Override
   public void handleInjection(ComponentRegistry registry) {
      registry.wireDependencies(function);
   }

   @Override
   public Stream<Stream<O>> map(Stream<I> iStream) {
      // Have to cast to make generics happy
      return iStream.map((Function<I, Stream<O>>) function);
   }

   @Override
   public Flowable<O> mapFlowable(Flowable<I> input) {
      return input.concatMap(o -> RxJavaInterop.fromStream(function.apply(o)));
   }
}
