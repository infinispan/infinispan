package org.infinispan.client.hotrod;

import java.util.function.Function;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.common.events.container.ContainerListenerEventType;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.sync.SyncCaches;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncLocks;
import org.infinispan.api.sync.SyncMultimaps;
import org.infinispan.api.sync.SyncStrongCounters;
import org.infinispan.api.sync.SyncWeakCounters;
import org.infinispan.api.sync.events.container.SyncContainerListener;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncContainer implements SyncContainer {
   private final HotRod hotrod;

   public HotRodSyncContainer(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public SyncContainer sync() {
      return this;
   }

   @Override
   public AsyncContainer async() {
      return hotrod.async();
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
   public SyncCaches caches() {
      return new HotRodSyncCaches(hotrod);
   }

   @Override
   public SyncMultimaps multimaps() {
      return new HotRodSyncMultimaps(hotrod);
   }

   @Override
   public SyncStrongCounters strongCounters() {
      return new HotRodSyncStrongCounters(hotrod);
   }

   @Override
   public SyncWeakCounters weakCounters() {
      return new HotRodSyncWeakCounters(hotrod);
   }

   @Override
   public SyncLocks locks() {
      return new HotRodSyncLocks(hotrod);
   }

   @Override
   public void listen(SyncContainerListener listener, ContainerListenerEventType... types) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> T batch(Function<SyncContainer, T> function) {
      throw new UnsupportedOperationException();
   }
}
