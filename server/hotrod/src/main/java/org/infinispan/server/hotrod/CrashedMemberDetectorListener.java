package org.infinispan.server.hotrod;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.context.Flag;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.logging.Log;

/**
 * Listener that detects crashed or stopped members and removes them from the address cache.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Listener(sync = false)
      // Use a separate thread to avoid blocking the view handler thread
class CrashedMemberDetectorListener {
   private final Cache<Address, ServerAddress> addressCache;

   private static final Log log = LogFactory.getLog(CrashedMemberDetectorListener.class, Log.class);

   CrashedMemberDetectorListener(Cache<Address, ServerAddress> cache, HotRodServer server) {
      // Let all nodes remove the address from their own cache locally.
      // This will exclude the address from their topology updates.
      this.addressCache = cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
   }

   @ViewChanged
   public void handleViewChange(ViewChangedEvent e) {
      detectCrashedMember(e);
   }

   void detectCrashedMember(ViewChangedEvent e) {
      try {
         if (addressCache.getStatus().allowInvocations()) {
            List<Address> goneMembers = e.getOldMembers().stream().filter(o -> !e.getNewMembers().contains(o)).collect(Collectors.toList());
            log.tracef("View change received: %s, removing members %s", e, goneMembers);
            // Consider doing removeAsync and then waiting for all removals...
            goneMembers.forEach(addressCache::remove);
         }
      } catch (Throwable t) {
         log.errorDetectingCrashedMember(t);
      }
   }

}
