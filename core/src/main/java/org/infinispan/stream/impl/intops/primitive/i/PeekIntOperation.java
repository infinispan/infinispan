package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

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
}
