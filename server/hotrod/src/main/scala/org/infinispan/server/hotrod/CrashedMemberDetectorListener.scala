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

/**
 * Listener that detects crashed or stopped members and removes them from
 * the address cache.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Listener(sync = false) // Use a separate thread to avoid blocking the view handler thread
class CrashedMemberDetectorListener(addressCache: Cache[Address, ServerAddress]) extends Log {

   @ViewChanged
   def handleViewChange(e: ViewChangedEvent) {
      val cacheManager = e.getCacheManager
      // Only the coordinator can potentially make modifications related to
      // crashed members. This is to avoid all nodes trying to make the same
      // modification which would be wasteful and lead to deadlocks.
      if (cacheManager.isCoordinator)
         detectCrashedMember(e)
   }

   private[hotrod] def detectCrashedMember(e: ViewChangedEvent) {
      trace("View change received on coordinator: %s", e)
      try {
         val newMembers = collectionAsScalaIterable(e.getNewMembers)
         val oldMembers = collectionAsScalaIterable(e.getOldMembers)
         val goneMembers = oldMembers.filterNot(newMembers contains _)
         goneMembers.foreach { addr =>
            trace("Remove %s from address cache", addr)
            addressCache.remove(addr)
         }
      } catch {
         case t: Throwable => logErrorDetectingCrashedMember(t)
      }
   }

}