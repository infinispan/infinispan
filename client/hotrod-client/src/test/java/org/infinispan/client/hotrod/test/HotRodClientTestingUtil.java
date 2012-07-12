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

package org.infinispan.client.hotrod.test;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.util.logging.LogFactory;

/**
 * Utility methods for the Hot Rod client
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class HotRodClientTestingUtil {

   private static final Log log = LogFactory.getLog(HotRodClientTestingUtil.class, Log.class);

   /**
    * Kills a remote cache manager.
    *
    * @param rcm the remote cache manager instance to kill
    */
   public static void killRemoteCacheManager(RemoteCacheManager rcm) {
      try {
         if (rcm != null) rcm.stop();
      } catch (Throwable t) {
         log.warn("Error stopping remote cache manager", t);
      }
   }

   /**
    * Kills a group of Hot Rod servers.
    *
    * @param servers the group of Hot Rod servers to kill
    */
   public static void killServers(HotRodServer... servers) {
      if (servers != null) {
         for (HotRodServer server : servers) {
            try {
               if (server != null) server.stop();
            } catch (Throwable t) {
               log.warn("Error stopping Hot Rod server", t);
            }
         }
      }
   }

   /**
    * Invoke a task using a remote cache manager. This method guarantees that
    * the remote manager used in the task will be cleaned up after the task has
    * completed, regardless of the task outcome.
    *
    * @param c task to execute
    * @throws Exception if the task fails somehow
    */
   public static void withRemoteCacheManager(RemoteCacheManagerCallable c)
         throws Exception {
      try {
         c.call();
      } finally {
         killRemoteCacheManager(c.rcm);
      }
   }

}
