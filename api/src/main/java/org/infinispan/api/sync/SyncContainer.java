package org.infinispan.api.sync;

import org.infinispan.api.Infinispan;
import org.infinispan.api.common.events.container.ContainerListenerEventType;
import org.infinispan.api.sync.events.container.SyncContainerListener;

/**
 * @since 14.0
 **/
public interface SyncContainer extends Infinispan {

   SyncCaches caches();

   SyncStrongCounters strongCounters();

   SyncWeakCounters weakCounters();

   SyncLocks locks();

   void listen(SyncContainerListener listener, ContainerListenerEventType... types);
}
