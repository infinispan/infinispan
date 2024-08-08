package org.infinispan.embedded;

import java.util.function.Function;

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
 * @since 15.0
 */
public class EmbeddedMutinyContainer implements MutinyContainer {
   private final Embedded embedded;

   EmbeddedMutinyContainer(Embedded embedded) {
      this.embedded = embedded;
   }

   @Override
   public SyncContainer sync() {
      return embedded.sync();
   }

   @Override
   public AsyncContainer async() {
      return embedded.async();
   }

   @Override
   public MutinyContainer mutiny() {
      return this;
   }

   @Override
   public void close() {
      embedded.close();
   }

   @Override
   public MutinyCaches caches() {
      return new EmbeddedMutinyCaches(embedded);
   }

   @Override
   public MutinyMultimaps multimaps() {
      return new EmbeddedMutinyMultimaps(embedded);
   }

   @Override
   public MutinyStrongCounters strongCounters() {
      return new EmbeddedMutinyStrongCounters(embedded);
   }

   @Override
   public MutinyWeakCounters weakCounters() {
      return new EmbeddedMutinyWeakCounters(embedded);
   }

   @Override
   public MutinyLocks locks() {
      return new EmbeddedMutinyLocks(embedded);
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
