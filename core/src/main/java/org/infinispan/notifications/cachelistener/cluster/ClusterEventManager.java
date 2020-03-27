package org.infinispan.notifications.cachelistener.cluster;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.infinispan.remoting.transport.Address;

public interface ClusterEventManager<K, V> {
   /**
    * Adds additional cluster events that need to be sent remotely for an event originating locally.
    * These events are batched by the {@code batchIdentifier} pending their submission when
    * {@link ClusterEventManager#sendEvents(Object)} is invoked or cancelled when {@link #dropEvents(Object)} is invoked.
    * @param batchIdentifier identifier for the batch
    * @param target The target node this event was meant for
    * @param identifier The cluster listener that is identified for these events
    * @param events The events that were generated
    * @param sync Whether these events need to be sent synchronously or not
    */
   void addEvents(Object batchIdentifier, Address target, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync);

   /**
    * Sends all previously added events for the given identifier
    */
   CompletionStage<Void> sendEvents(Object batchIdentifier);

   /**
    * Drops and ignores all previously added events for the given identifier.
    */
   void dropEvents(Object batchIdentifier);
}
