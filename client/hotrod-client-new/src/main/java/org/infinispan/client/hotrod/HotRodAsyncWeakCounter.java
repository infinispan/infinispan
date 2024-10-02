package org.infinispan.client.hotrod;

import java.util.concurrent.CompletionStage;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.async.AsyncWeakCounter;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodAsyncWeakCounter implements AsyncWeakCounter {
   private final HotRod hotrod;
   private final String name;

   HotRodAsyncWeakCounter(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public CompletionStage<CounterConfiguration> configuration() {
      throw new UnsupportedOperationException();
   }

   @Override
   public AsyncContainer container() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Long> value() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> add(long delta) {
      throw new UnsupportedOperationException();
   }
}
