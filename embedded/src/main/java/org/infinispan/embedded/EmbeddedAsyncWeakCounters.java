package org.infinispan.embedded;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncWeakCounter;
import org.infinispan.api.async.AsyncWeakCounters;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;

/**
 * @since 15.0
 */
public class EmbeddedAsyncWeakCounters implements AsyncWeakCounters {
   private final Embedded embedded;
   private final CounterManager counterManager;

   EmbeddedAsyncWeakCounters(Embedded embedded) {
      this.embedded = embedded;
      this.counterManager = EmbeddedCounterManagerFactory.asCounterManager(embedded.cacheManager);
   }

   @Override
   public CompletionStage<AsyncWeakCounter> get(String name) {
      return null;
   }

   @Override
   public CompletionStage<AsyncWeakCounter> create(String name, CounterConfiguration configuration) {
      return null;
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      return null;
   }

   @Override
   public Flow.Publisher<String> names() {
      return null;
   }
}
