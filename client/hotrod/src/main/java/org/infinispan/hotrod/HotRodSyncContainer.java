package org.infinispan.hotrod;

import java.util.function.Function;

import org.infinispan.api.common.events.container.ContainerListenerEventType;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.events.container.SyncContainerListener;

/**
 * @since 14.0
 **/
public class HotRodSyncContainer implements SyncContainer {
   private final HotRod hotrod;

   public HotRodSyncContainer(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public HotRodSyncContainer sync() {
      return this;
   }

   @Override
   public HotRodAsyncContainer async() {
      return hotrod.async();
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
   public HotRodSyncCaches caches() {
      return new HotRodSyncCaches(hotrod);
   }

   @Override
   public HotRodSyncMultimaps multimaps() {
      return new HotRodSyncMultimaps(hotrod);
   }

   @Override
   public HotRodSyncStrongCounters strongCounters() {
      return new HotRodSyncStrongCounters(hotrod);
   }

   @Override
   public HotRodSyncWeakCounters weakCounters() {
      return new HotRodSyncWeakCounters(hotrod);
   }

   @Override
   public HotRodSyncLocks locks() {
      return new HotRodSyncLocks(hotrod);
   }

   @Override
   public void listen(SyncContainerListener listener, ContainerListenerEventType... types) {

   }

   @Override
   public <T> T batch(Function<SyncContainer, T> function) {
      return null;
   }
}
