package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs peek operation on a {@link IntStream}
 */
public class PeekIntOperation implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {
   private final IntConsumer consumer;

   public PeekIntOperation(IntConsumer consumer) {
      this.consumer = consumer;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.peek(consumer);
   }

   public IntConsumer getConsumer() {
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
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input.doOnNext(consumer::accept);
   }
}
