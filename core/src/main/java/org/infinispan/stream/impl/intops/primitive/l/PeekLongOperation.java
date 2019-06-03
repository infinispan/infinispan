package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongConsumer;
import java.util.stream.LongStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.Flowable;

/**
 * Performs peek operation on a {@link LongStream}
 */
public class PeekLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private final LongConsumer consumer;

   public PeekLongOperation(LongConsumer consumer) {
      this.consumer = consumer;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.peek(consumer);
   }

   public LongConsumer getConsumer() {
      return consumer;
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.doOnNext(consumer::accept);
   }
}
