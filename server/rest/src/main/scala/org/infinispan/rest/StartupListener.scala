package org.infinispan.rest

import org.infinispan.manager.DefaultCacheManager
import scala.collection.JavaConversions._
import javax.servlet.{ServletConfig, ServletContextListener, ServletContextEvent}
import javax.servlet.http.HttpServlet

/**
 * To init the cache manager. Nice to do this on startup as any config problems will be picked up before any
 * requests are attempted to be serviced. Less kitten carnage.
 *
 * @author Michael Neale
 * @author Galder Zamarreño
 * @since 4.0
 */
class StartupListener extends HttpServlet {
   override def init(cfg: ServletConfig) {
      super.init(cfg)
      val cfgFile = cfg getInitParameter "infinispan.config"
      if (cfgFile == null)
         ManagerInstance.instance = new DefaultCacheManager
      else {
         ManagerInstance.instance = new DefaultCacheManager(cfgFile)
      }

     // Start defined caches to avoid issues with lazily started caches
     for (cacheName <- asIterator(ManagerInstance.instance.getCacheNames.iterator))
        ManagerInstance.instance.getCache(cacheName)
     // Finally, start default cache as well
     ManagerInstance.instance.getCache[String, Any]
  }
}