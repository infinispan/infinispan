package org.infinispan.affinity;

import org.infinispan.affinity.impl.KeyAffinityServiceImpl;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;

/**
* Used for registering various cache notifications.
*
* @author Mircea.Markus@jboss.com
* @since 4.1
*/
@Listener(sync = true)
public class ListenerRegistration {
   private final KeyAffinityServiceImpl<?> keyAffinityService;

   public ListenerRegistration(KeyAffinityServiceImpl<?> keyAffinityService) {
      this.keyAffinityService = keyAffinityService;
   }

   @TopologyChanged
   public void handleViewChange(TopologyChangedEvent<?, ?> tce) {
      if (!tce.isPre()) keyAffinityService.handleViewChange(tce);
   }

   @CacheStopped
   public void handleCacheStopped(CacheStoppedEvent cse) {
      keyAffinityService.handleCacheStopped(cse);
   }
}
