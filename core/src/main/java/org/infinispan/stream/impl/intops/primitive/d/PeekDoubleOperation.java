package org.infinispan.stream.impl.intops.primitive.d;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.DoubleConsumer;
import java.util.stream.DoubleStream;

/**
 * Performs peek operation on a {@link DoubleStream}
 */
public class PeekDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private final DoubleConsumer consumer;

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
}
