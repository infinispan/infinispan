package org.infinispan.hotrod;

import java.util.concurrent.CompletionStage;

import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.async.AsyncWeakCounter;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
public class HotRodAsyncWeakCounter implements AsyncWeakCounter {
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
      return null;
   }

   @Override
   public AsyncContainer container() {
      return null;
   }

   @Override
   public CompletionStage<Long> value() {
      return null;
   }

   @Override
   public CompletionStage<Void> add(long delta) {
      return null;
   }
}
