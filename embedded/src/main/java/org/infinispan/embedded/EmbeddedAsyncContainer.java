package org.infinispan.embedded;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import org.infinispan.api.async.AsyncCaches;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.async.AsyncLocks;
import org.infinispan.api.async.AsyncMultimaps;
import org.infinispan.api.async.AsyncStrongCounters;
import org.infinispan.api.async.AsyncWeakCounters;
import org.infinispan.api.common.events.container.ContainerEvent;
import org.infinispan.api.common.events.container.ContainerListenerEventType;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.sync.SyncContainer;

/**
 * @since 15.0
 */
public class EmbeddedAsyncContainer implements AsyncContainer {
   private final Embedded embedded;

   EmbeddedAsyncContainer(Embedded embedded) {
      this.embedded = embedded;
   }

   @Override
   public SyncContainer sync() {
      return embedded.sync();
   }

   @Override
   public AsyncContainer async() {
      return this;
   }

   @Override
   public MutinyContainer mutiny() {
      return embedded.mutiny();
   }

   @Override
   public void close() {
      embedded.close();
   }

   @Override
   public AsyncCaches caches() {
      return new EmbeddedAsyncCaches(embedded);
   }

   @Override
   public AsyncMultimaps multimaps() {
      return new EmbeddedAsyncMultimaps(embedded);
   }

   @Override
   public AsyncStrongCounters strongCounters() {
      return new EmbeddedAsyncStrongCounters(embedded);
   }

   @Override
   public AsyncWeakCounters weakCounters() {
      return new EmbeddedAsyncWeakCounters(embedded);
   }

   @Override
   public AsyncLocks locks() {
      return new EmbeddedAsyncLocks(embedded);
   }

   @Override
   public Flow.Publisher<ContainerEvent> listen(ContainerListenerEventType... types) {
      return null;
   }

   @Override
   public <T> CompletionStage<T> batch(Function<AsyncContainer, CompletionStage<T>> function) {
      return null;
   }
}
