package org.infinispan.server.hotrod

import org.infinispan.commons.logging.LogFactory
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent

import scala.collection.JavaConversions._
import org.infinispan.context.Flag
import org.infinispan.server.hotrod.logging.JavaLog

/**
 * Listener that detects crashed or stopped members and removes them from
 * the address cache.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Listener(sync = false) // Use a separate thread to avoid blocking the view handler thread
class CrashedMemberDetectorListener(cache: AddressCache, server: HotRodServer) {

   // Let all nodes remove the address from their own cache locally.
   // This will exclude the address from their topology updates.
   val addressCache = cache.getAdvancedCache.withFlags(Flag.CACHE_MODE_LOCAL)
   val log = LogFactory.getLog(getClass, classOf[JavaLog])

   @ViewChanged
   def handleViewChange(e: ViewChangedEvent) {
      detectCrashedMember(e)
   }

   private[hotrod] def detectCrashedMember(e: ViewChangedEvent) {
      try {
         val newMembers = collectionAsScalaIterable(e.getNewMembers)
         val oldMembers = collectionAsScalaIterable(e.getOldMembers)
         val goneMembers = oldMembers.filterNot(newMembers contains _)
         log.tracef("View change received: %s, removing members %s", Array(e, goneMembers).map(_.asInstanceOf[AnyRef]) : _*)
         // Consider doing removeAsync and then waiting for all removals...
         goneMembers.foreach(a => if (addressCache.getStatus.allowInvocations()) addressCache.remove(a))
      } catch {
         case t: Throwable => log.errorDetectingCrashedMember(t)
      }
   }

}
