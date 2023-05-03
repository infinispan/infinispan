package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import org.infinispan.api.Infinispan;
import org.infinispan.api.common.events.container.ContainerEvent;
import org.infinispan.api.common.events.container.ContainerListenerEventType;

/**
 * @since 14.0
 **/
public interface AsyncContainer extends Infinispan {

   AsyncCaches caches();

   AsyncMultimaps multimaps();

   AsyncStrongCounters strongCounters();

   AsyncWeakCounters weakCounters();

   AsyncLocks locks();

   Flow.Publisher<ContainerEvent> listen(ContainerListenerEventType... types);

   <T> CompletionStage<T> batch(Function<AsyncContainer, CompletionStage<T>> function);
}
