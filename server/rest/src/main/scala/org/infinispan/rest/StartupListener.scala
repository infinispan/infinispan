package org.infinispan.rest


import javax.servlet.{ServletContextListener, ServletContextEvent}
import org.infinispan.manager.DefaultCacheManager


/**
 * To init the cache manager. Nice to do this on startup as any config problems will be picked up before any
 * requests are attempted to be serviced. Less kitten carnage.
 */
class StartupListener extends ServletContextListener {
  def contextInitialized(ev: ServletContextEvent) = {
    // Start with the default config (LOCAL mode!)  TODO - add the ability to specify a config file when deploying (ISPN-281)
    ManagerInstance.instance = new DefaultCacheManager()
    ManagerInstance.instance.start
  }
  def contextDestroyed(ev: ServletContextEvent) = {
    ManagerInstance.instance.stop
  }
}