package org.infinispan.client.hotrod;

import java.util.function.Function;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.common.events.container.ContainerEvent;
import org.infinispan.api.common.events.container.ContainerListenerEventType;
import org.infinispan.api.mutiny.MutinyCaches;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.mutiny.MutinyLocks;
import org.infinispan.api.mutiny.MutinyMultimaps;
import org.infinispan.api.mutiny.MutinyStrongCounters;
import org.infinispan.api.mutiny.MutinyWeakCounters;
import org.infinispan.api.sync.SyncContainer;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodMutinyContainer implements MutinyContainer {
   private final HotRod hotrod;

   HotRodMutinyContainer(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public SyncContainer sync() {
      return hotrod.sync();
   }

   @Override
   public AsyncContainer async() {
      return hotrod.async();
   }

   @Override
   public MutinyContainer mutiny() {
      return this;
   }

   @Override
   public void close() {
      hotrod.close();
   }

   @Override
   public MutinyCaches caches() {
      return new HotRodMutinyCaches(hotrod);
   }

   @Override
   public MutinyMultimaps multimaps() {
      return new HotRodMutinyMultimaps(hotrod);
   }

   @Override
   public MutinyStrongCounters strongCounters() {
      return new HotRodMutinyStrongCounters(hotrod);
   }

   @Override
   public MutinyWeakCounters weakCounters() {
      return new HotRodMutinyWeakCounters(hotrod);
   }

   @Override
   public MutinyLocks locks() {
      return new HotRodMutinyLocks(hotrod);
   }

   @Override
   public Multi<ContainerEvent> listen(ContainerListenerEventType... types) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <R> Uni<R> execute(String name, Object... args) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Uni<T> batch(Function<MutinyContainer, Uni<T>> function) {
      throw new UnsupportedOperationException();
   }
}
