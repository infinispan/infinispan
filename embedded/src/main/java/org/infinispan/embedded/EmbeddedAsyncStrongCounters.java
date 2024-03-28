package org.infinispan.embedded;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncStrongCounter;
import org.infinispan.api.async.AsyncStrongCounters;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;

/**
 * @since 15.0
 */
public class EmbeddedAsyncStrongCounters implements AsyncStrongCounters {
   private final Embedded embedded;
   private final CounterManager counterManager;

   EmbeddedAsyncStrongCounters(Embedded embedded) {
      this.embedded = embedded;
      this.counterManager = EmbeddedCounterManagerFactory.asCounterManager(embedded.cacheManager);
   }

   @Override
   public CompletionStage<AsyncStrongCounter> get(String name) {
      return null;
   }

   @Override
   public CompletionStage<AsyncStrongCounter> create(String name, CounterConfiguration configuration) {
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
