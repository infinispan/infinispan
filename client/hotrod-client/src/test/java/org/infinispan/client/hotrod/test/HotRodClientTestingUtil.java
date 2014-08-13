package org.infinispan.client.hotrod.test;

import org.infinispan.client.hotrod.RemoteCache;
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
    * Kills a group of remote cache managers.
    *
    * @param rcm
    *           the remote cache manager instances to kill
    */
   public static void killRemoteCacheManagers(RemoteCacheManager... rcms) {
      if (rcms != null) {
         for (RemoteCacheManager rcm : rcms) {
            try {
               if (rcm != null)
                  rcm.stop();
            } catch (Throwable t) {
               log.warn("Error stopping remote cache manager", t);
            }
         }
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
   public static void withRemoteCacheManager(RemoteCacheManagerCallable c) {
      try {
         c.call();
      } finally {
         killRemoteCacheManager(c.rcm);
      }
   }

   public static <K, V> void withClientListener(Object listener, RemoteCacheManagerCallable c) {
      RemoteCache<K, V> cache = c.rcm.getCache();
      cache.addClientListener(listener);
      try {
         c.call();
      } finally {
         cache.removeClientListener(listener);
      }
   }

   public static <K, V> void withClientListener(Object listener,
         Object[] filterFactoryParams, Object[] converterFactoryParams, RemoteCacheManagerCallable c) {
      RemoteCache<K, V> cache = c.rcm.getCache();
      cache.addClientListener(listener, filterFactoryParams, converterFactoryParams);
      try {
         c.call();
      } finally {
         cache.removeClientListener(listener);
      }
   }

}
