package org.infinispan.server.hotrod

import logging.Log
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachemanagerlistener.annotation.{Merged, ViewChanged}
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent
import scala.collection.JavaConversions._
import org.infinispan.context.Flag

/**
 * Listener that detects crashed, stopped and merged members. When
 * crashed/stopped members are detected, these are removed from the address
 * cache but they're temporarily cached locally in case the nodes get merged
 * back.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Listener(sync = false) // Use a separate thread to avoid blocking the view handler thread
class CrashedMemberDetectorListener(cache: AddressCache, backupCache: AddressCache, server: HotRodServer) extends Log {

   // Let all nodes remove the address from their own cache locally. By doing
   // this, we can guarantee that transport view id has been updated before
   // updating the locally cached view id. This cached view id is used to
   // guarantee that the address cache will be updated *before* the client can
   // detect a new topology view.
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
         if (!goneMembers.isEmpty) {
            // Consider doing removeAsync and then waiting for all removals...
            if (addressCache.getStatus.allowInvocations()) {
               goneMembers.foreach(addr => {
                  val serverAddr = addressCache.get(addr)
                  backupCache.put(addr, serverAddr)
                  addressCache.remove(addr)
               })
            }
         }
      } catch {
         case t: Throwable => logErrorDetectingCrashedMember(t)
      }
   }

   @Merged
   def handleViewMerged(e: ViewChangedEvent) {
      trace("Merge view change received: %s", e)
      val newMembers = collectionAsScalaIterable(e.getNewMembers)
      val oldMembers = collectionAsScalaIterable(e.getOldMembers)
      val addedMembers = newMembers.filterNot(oldMembers contains _)
      if (addressCache.getStatus.allowInvocations()) {
         trace("Added members are: %s", addedMembers)
         if (addedMembers.nonEmpty) {
            addedMembers.foreach(added => {
               val serverAddr = backupCache.get(added)
               if (serverAddr != null)
                  addressCache.put(added, serverAddr)
               else
                  logMergedMemberCantBeFound(added.toString)
            })
         }
         val removedMembers = oldMembers.filterNot(newMembers contains _)
         trace("Removed members are: %s", removedMembers)
         if (removedMembers.nonEmpty)
            removedMembers.foreach(removed => addressCache.remove(removed))
      }
   }

}
