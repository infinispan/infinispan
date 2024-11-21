package org.infinispan.client.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import org.infinispan.api.Experimental;
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
 * @since 14.0
 **/
@Experimental
final class HotRodAsyncContainer implements AsyncContainer {
   private final HotRod hotrod;

   HotRodAsyncContainer(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public SyncContainer sync() {
      return hotrod.sync();
   }

   @Override
   public AsyncContainer async() {
      return this;
   }

   @Override
   public MutinyContainer mutiny() {
      return hotrod.mutiny();
   }

   @Override
   public void close() {
      hotrod.close();
   }

   @Override
   public AsyncCaches caches() {
      return new HotRodAsyncCaches(hotrod);
   }

   @Override
   public AsyncMultimaps multimaps() {
      return new HotRodAsyncMultimaps(hotrod);
   }

   @Override
   public AsyncStrongCounters strongCounters() {
      return new HotRodAsyncStrongCounters(hotrod);
   }

   @Override
   public AsyncWeakCounters weakCounters() {
      return new HotRodAsyncWeakCounters(hotrod);
   }

   @Override
   public AsyncLocks locks() {
      return new HotRodAsyncLocks(hotrod);
   }

   @Override
   public Flow.Publisher<ContainerEvent> listen(ContainerListenerEventType... types) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> CompletionStage<T> batch(Function<AsyncContainer, CompletionStage<T>> function) {
      throw new UnsupportedOperationException();
   }
}
