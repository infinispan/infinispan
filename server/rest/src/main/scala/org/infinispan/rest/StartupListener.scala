package org.infinispan.rest


import javax.servlet.{ServletContextListener, ServletContextEvent}
import manager.DefaultCacheManager


/**
 * To init the cache manager. Nice to do this on startup as any config problems will be picked up before any
 * requests are attempted to be serviced. Less kitten carnage.
 */
class StartupListener extends ServletContextListener {
  def contextInitialized(ev: ServletContextEvent) = {
    ManagerInstance.instance = new DefaultCacheManager("infinispan-config.xml")
    ManagerInstance.instance.start
  }
  def contextDestroyed(ev: ServletContextEvent) = {
    ManagerInstance.instance.stop
  }
}