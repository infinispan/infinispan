package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.concurrent.BlockingManager;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * Performs peek operation on a {@link IntStream}
 */
public class PeekIntOperation implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {
   private final IntConsumer consumer;
   private BlockingManager blockingManager;

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
      blockingManager = registry.getBlockingManager().running();
      if (consumer instanceof CacheAware) {
         ((CacheAware) consumer).injectCache(registry.getCache().running());
      } else {
         registry.wireDependencies(consumer);
      }
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input.concatMapSingle(t -> Single.fromCompletionStage(
            blockingManager.supplyBlocking(() -> {
               consumer.accept(t);
               return t;
            }, "publisher-peek")));
   }
}
