package org.infinispan.hotrod;

import java.util.function.Function;

import org.infinispan.api.common.events.container.ContainerEvent;
import org.infinispan.api.common.events.container.ContainerListenerEventType;
import org.infinispan.api.mutiny.MutinyContainer;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyContainer implements MutinyContainer {
   private final HotRod hotrod;

   public HotRodMutinyContainer(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public HotRodSyncContainer sync() {
      return hotrod.sync();
   }

   @Override
   public HotRodAsyncContainer async() {
      return hotrod.async();
   }

   @Override
   public HotRodMutinyContainer mutiny() {
      return this;
   }

   @Override
   public void close() {
      hotrod.close();
   }

   @Override
   public HotRodMutinyCaches caches() {
      return new HotRodMutinyCaches(hotrod);
   }

   @Override
   public HotRodMutinyMultiMaps multiMaps() {
      return new HotRodMutinyMultiMaps(hotrod);
   }

   @Override
   public HotRodMutinyStrongCounters strongCounters() {
      return new HotRodMutinyStrongCounters(hotrod);
   }

   @Override
   public HotRodMutinyWeakCounters weakCounters() {
      return new HotRodMutinyWeakCounters(hotrod);
   }

   @Override
   public HotRodMutinyLocks locks() {
      return new HotRodMutinyLocks(hotrod);
   }

   @Override
   public Multi<ContainerEvent> listen(ContainerListenerEventType... types) {
      return null;
   }

   @Override
   public <R> Uni<R> execute(String name, Object... args) {
      return null;
   }

   @Override
   public <T> Uni<T> batch(Function<MutinyContainer, Uni<T>> function) {
      return null;
   }
}
