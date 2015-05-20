package org.infinispan.stream.impl.intops.object;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.function.Consumer;
import java.util.stream.Stream;

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
}
