package org.infinispan.server.hotrod

import logging.Log
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent
import scala.collection.JavaConversions._
import org.infinispan.context.Flag

/**
 * Listener that detects crashed or stopped members and removes them from
 * the address cache.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Listener(sync = false) // Use a separate thread to avoid blocking the view handler thread
class CrashedMemberDetectorListener(cache: AddressCache, server: HotRodServer) extends Log {

   // Let all nodes remove the address from their own cache locally.
   // This will exclude the address from their topology updates.
   val addressCache = cache.getAdvancedCache.withFlags(Flag.CACHE_MODE_LOCAL)

   @ViewChanged
   def handleViewChange(e: ViewChangedEvent) {
      detectCrashedMember(e)
   }

   private[hotrod] def detectCrashedMember(e: ViewChangedEvent) {
      try {
         val newMembers = collectionAsScalaIterable(e.getNewMembers)
         val oldMembers = collectionAsScalaIterable(e.getOldMembers)
         val goneMembers = oldMembers.filterNot(newMembers contains _)
         trace("View change received: %s, removing members %s", e, goneMembers)
         // Consider doing removeAsync and then waiting for all removals...
         goneMembers.foreach(a => if (addressCache.getStatus.allowInvocations()) addressCache.remove(a))
      } catch {
         case t: Throwable => logErrorDetectingCrashedMember(t)
      }
   }

}
