package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongConsumer;
import java.util.stream.LongStream;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.concurrent.BlockingManager;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * Performs peek operation on a {@link LongStream}
 */
public class PeekLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private final LongConsumer consumer;
   private BlockingManager blockingManager;

   public PeekLongOperation(LongConsumer consumer) {
      this.consumer = consumer;
   }

   @ProtoFactory
   PeekLongOperation(MarshallableObject<LongConsumer> consumer) {
      this.consumer = MarshallableObject.unwrap(consumer);
   }

   @ProtoField(1)
   MarshallableObject<LongConsumer> getConsumer() {
      return MarshallableObject.create(consumer);
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.peek(consumer);
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
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.concatMapSingle(t -> Single.fromCompletionStage(
            blockingManager.supplyBlocking(() -> {
               consumer.accept(t);
               return t;
            }, "publisher-peek")));
   }
}
