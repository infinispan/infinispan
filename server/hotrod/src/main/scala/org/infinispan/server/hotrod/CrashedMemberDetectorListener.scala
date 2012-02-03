/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.server.hotrod

import logging.Log
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent
import scala.collection.JavaConversions._
import org.infinispan.Cache
import org.infinispan.remoting.transport.Address
import org.infinispan.context.Flag

/**
 * Listener that detects crashed or stopped members and removes them from
 * the address cache.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Listener(sync = false) // Use a separate thread to avoid blocking the view handler thread
class CrashedMemberDetectorListener(cache: Cache[Address, ServerAddress], server: HotRodServer) extends Log {

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
      trace("View change received: %s", e)
      try {
         val newMembers = collectionAsScalaIterable(e.getNewMembers)
         val oldMembers = collectionAsScalaIterable(e.getOldMembers)
         val goneMembers = oldMembers.filterNot(newMembers contains _)
         // Consider doing removeAsync and then waiting for all removals...
         goneMembers.foreach { addr =>
            trace("Remove %s from address cache", addr)
            addressCache.remove(addr)
         }
         updateViewdId(e)
      } catch {
         case t: Throwable => logErrorDetectingCrashedMember(t)
      }
   }

   protected def updateViewdId(e: ViewChangedEvent) {
      // Multiple members could leave at the same time, so delay any view id
      // updates until the all addresses have been removed from memory.
      server.setViewId(e.getViewId)
   }

}