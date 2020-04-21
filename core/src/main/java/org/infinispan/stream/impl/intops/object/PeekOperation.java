package org.infinispan.stream.impl.intops.object;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs peek operation on a regular {@link Stream}
 */
public class PeekOperation<S> implements IntermediateOperation<S, Stream<S>, S, Stream<S>> {
   private final Consumer<? super S> consumer;

   public PeekOperation(Consumer<? super S> consumer) {
      this.consumer = consumer;
   }

   @Override
   public Stream<S> perform(Stream<S> stream) {
      return stream.peek(consumer);
   }

   public Consumer<? super S> getConsumer() {
      return consumer;
   }

   @Override
   public void handleInjection(ComponentRegistry registry) {
      if (consumer instanceof CacheAware) {
         ((CacheAware) consumer).injectCache(registry.getCache().running());
      } else {
         registry.wireDependencies(consumer);
      }
   }

   @Override
   public Flowable<S> mapFlowable(Flowable<S> input) {
      return input.doOnNext(consumer::accept);
   }
}
