package org.infinispan.notifications.cachelistener.cluster;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.Address;

public interface ClusterEventManager<K, V> {
   /**
    * Adds additional cluster events that need to be sent remotely for an event originating locally.
    * These events are not sent at time of registering but rather after the {@link ClusterEventManager#sendEvents()} is invoked.
    * These events are gathered on a per thread basis and batched to reduce number of RPCs required.
    * @param target The target node this event was meant for
    * @param identifier The cluster listener that is identified for these events
    * @param events The events that were generated
    * @param sync Whether these events need to be sent synchronously or not
    */
   void addEvents(Address target, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync);

   /**
    * Sends all previously added events on this thread
    */
   CompletionStage<Void> sendEvents() throws CacheException;

   /**
    * Drops and ignores all previously added events on this thread.
    */
   void dropEvents();
}
