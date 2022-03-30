package org.infinispan.api.mutiny;

import java.util.function.Function;

import org.infinispan.api.Infinispan;
import org.infinispan.api.common.events.container.ContainerEvent;
import org.infinispan.api.common.events.container.ContainerListenerEventType;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public interface MutinyContainer extends Infinispan {
   MutinyCaches caches();

   MutinyMultiMaps multiMaps();

   MutinyStrongCounters strongCounters();

   MutinyWeakCounters weakCounters();

   MutinyLocks locks();

   /**
    * @param types
    * @return
    */
   Multi<ContainerEvent> listen(ContainerListenerEventType... types);

   <R> Uni<R> execute(String name, Object... args);

   <T> Uni<T> batch(Function<MutinyContainer, Uni<T>> function);
}
