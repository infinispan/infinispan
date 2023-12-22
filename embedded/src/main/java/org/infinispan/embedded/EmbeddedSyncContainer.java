package org.infinispan.embedded;

import java.util.function.Function;

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
 * @since 15.0
 */
public class EmbeddedSyncContainer implements SyncContainer {
   private final Embedded embedded;

   EmbeddedSyncContainer(Embedded embedded) {
      this.embedded = embedded;
   }

   @Override
   public SyncContainer sync() {
      return this;
   }

   @Override
   public AsyncContainer async() {
      return embedded.async();
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
   public SyncCaches caches() {
      return new EmbeddedSyncCaches(embedded);
   }

   @Override
   public SyncMultimaps multimaps() {
      return new EmbeddedSyncMultimaps(embedded);
   }

   @Override
   public SyncStrongCounters strongCounters() {
      return new EmbeddedSyncStrongCounters(embedded);
   }

   @Override
   public SyncWeakCounters weakCounters() {
      return new EmbeddedSyncWeakCounters(embedded);
   }

   @Override
   public SyncLocks locks() {
      return new EmbeddedSyncLocks(embedded);
   }

   @Override
   public void listen(SyncContainerListener listener, ContainerListenerEventType... types) {

   }

   @Override
   public <T> T batch(Function<SyncContainer, T> function) {
      return null;
   }
}
