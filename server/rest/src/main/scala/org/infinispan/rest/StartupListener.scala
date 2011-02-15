package org.infinispan.rest

import scala.collection.JavaConversions._
import javax.servlet.http.HttpServlet
import org.infinispan.manager.{EmbeddedCacheManager, DefaultCacheManager}
import org.infinispan.server.core.Logging
import javax.servlet.{ServletContext, ServletConfig}

/**
 * To init the cache manager. Nice to do this on startup as any config problems will be picked up before any
 * requests are attempted to be serviced. Less kitten carnage.
 *
 * @author Michael Neale
 * @author Galder Zamarreño
 * @since 4.0
 */
class StartupListener extends HttpServlet with Logging {
   override def init(cfg: ServletConfig) {
      super.init(cfg)

      // Check whether the listener is running within an MC environment. A couple of reasons to do this here:
      // 1. REST module is a war file, so a module can't depend on it and compile against it
      //    and any module that would want to provide a different listener would need to end up
      //    setting ManagerInstance.instance
      // 2. Doing it here makes it a lot easier rather than needing to have a separate module
      //    within app server build system
      ManagerInstance.instance = getMcInjectedCacheManager(cfg)

      // If cache manager is still null, create one for REST server's own usage
      if (ManagerInstance.instance == null) {
         val cfgFile = cfg getInitParameter "infinispan.config"
         if (cfgFile == null)
            ManagerInstance.instance = new DefaultCacheManager
         else
            ManagerInstance.instance = new DefaultCacheManager(cfgFile)
      }

      // Start defined caches to avoid issues with lazily started caches
      for (cacheName <- asIterator(ManagerInstance.instance.getCacheNames.iterator))
         ManagerInstance.instance.getCache(cacheName)
      // Finally, start default cache as well
      ManagerInstance.instance.getCache[String, Any]
   }

   /**
    * To avoid any hard dependencies, checking whether the cache manager is injected
    * via the JBoss MicroContainer is done using reflection.
    */
   private def getMcInjectedCacheManager(cfg: ServletConfig): EmbeddedCacheManager = {
      val isDebug = isDebugEnabled
      val kernel = cfg.getServletContext.getAttribute("jboss.kernel:service=Kernel")
      if (kernel != null) {
         val kernelCl = loadClass("org.jboss.kernel.Kernel")
         val kernelReg = kernelCl.getMethod("getRegistry").invoke(kernel)
         val kernelRegCl = loadClass("org.jboss.kernel.spi.registry.KernelRegistry")
         var beanName = cfg.getInitParameter("infinispan.cachemanager.bean")
         if (beanName == null)
            beanName = "DefaultCacheManager"

         val kernelRegEntry = kernelRegCl.getMethod("findEntry", classOf[Object]).invoke(kernelReg, beanName)
         if (kernelRegEntry != null) {
            val kernelRegEntryCl = loadClass("org.jboss.kernel.spi.registry.KernelRegistryEntry")
            if (isDebug) debug("Retrieving cache manager from JBoss Microcontainer")
            return kernelRegEntryCl.getMethod("getTarget").invoke(kernelRegEntry).asInstanceOf[EmbeddedCacheManager]
         } else {
            if (isDebug) debug("Running within JBoss Microcontainer but cache manager bean not present")
            return null
         }
      }
      return null
   }

   private def loadClass(name: String): Class[_] = {
      val clazz = Thread.currentThread.getContextClassLoader loadClass name
      clazz.asInstanceOf[Class[_]]
   }

}