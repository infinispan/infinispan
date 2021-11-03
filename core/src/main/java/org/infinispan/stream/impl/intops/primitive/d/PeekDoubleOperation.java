package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleConsumer;
import java.util.stream.DoubleStream;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.concurrent.BlockingManager;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * Performs peek operation on a {@link DoubleStream}
 */
public class PeekDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private final DoubleConsumer consumer;
   private BlockingManager blockingManager;

   public PeekDoubleOperation(DoubleConsumer consumer) {
      this.consumer = consumer;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.peek(consumer);
   }

   public DoubleConsumer getConsumer() {
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
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.concatMapSingle(t -> Single.fromCompletionStage(
            blockingManager.supplyBlocking(() -> {
               consumer.accept(t);
               return t;
            }, "publisher-peek")));
   }
}
