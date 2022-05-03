package org.infinispan.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.async.AsyncStrongCounters;
import org.infinispan.api.common.events.container.ContainerEvent;
import org.infinispan.api.common.events.container.ContainerListenerEventType;

/**
 * @since 14.0
 **/
public class HotRodAsyncContainer implements AsyncContainer {
   private final HotRod hotrod;

   HotRodAsyncContainer(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public HotRodSyncContainer sync() {
      return hotrod.sync();
   }

   @Override
   public HotRodAsyncContainer async() {
      return this;
   }

   @Override
   public HotRodMutinyContainer mutiny() {
      return hotrod.mutiny();
   }

   @Override
   public void close() {
      hotrod.close();
   }

   @Override
   public HotRodAsyncCaches caches() {
      return new HotRodAsyncCaches(hotrod);
   }

   @Override
   public HotRodAsyncMultiMaps multiMaps() {
      return new HotRodAsyncMultiMaps(hotrod);
   }

   @Override
   public AsyncStrongCounters strongCounters() {
      return new HotRodAsyncStrongCounters(hotrod);
   }

   @Override
   public HotRodAsyncWeakCounters weakCounters() {
      return new HotRodAsyncWeakCounters(hotrod);
   }

   @Override
   public HotRodAsyncLocks locks() {
      return new HotRodAsyncLocks(hotrod);
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
