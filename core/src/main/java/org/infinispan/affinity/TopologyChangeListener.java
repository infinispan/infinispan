package org.infinispan.affinity;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.BlockingQueue;

/**
* // TODO: Document this
*
* @author Mircea.Markus@jboss.com
* @since 4.1
*/
@Listener
class TopologyChangeListener {
   private final KeyAffinityServiceImpl keyAffinityService;

   public TopologyChangeListener(KeyAffinityServiceImpl keyAffinityService) {
      this.keyAffinityService = keyAffinityService;
   }

   @ViewChanged
   public void handleViewChange(ViewChangedEvent vce) {
      keyAffinityService.handleViewChange(vce);
   }
}
