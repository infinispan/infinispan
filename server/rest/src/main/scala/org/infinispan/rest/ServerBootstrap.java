/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.rest;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.logging.JavaLog;
import org.infinispan.util.logging.LogFactory;

/**
 * Initializes cache manager for the REST server and sets it into the servlet context.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 * @since 5.2
 */
public class ServerBootstrap implements ServletContextListener {

   private final static JavaLog log = LogFactory.getLog(ServerBootstrap.class, JavaLog.class);

   public final static String MANAGER = "org.infinispan.rest.ServerBootstrap.MANAGER";

   private void setManager(ServletContextEvent sce, EmbeddedCacheManager cacheManager) {
      setToServletContext(sce.getServletContext(), cacheManager);
   }

   private EmbeddedCacheManager getManager(ServletContextEvent sce) {
      ManagerInstance managerInstance = (ManagerInstance) sce.getServletContext().getAttribute(MANAGER);
      return managerInstance == null ? null : managerInstance.getInstance();
   }

   public static void setToServletContext(ServletContext servletContext, EmbeddedCacheManager cacheManager) {
      servletContext.setAttribute(MANAGER, new ManagerInstance(cacheManager));
   }

   @Override
   public void contextInitialized(ServletContextEvent sce) {
      EmbeddedCacheManager cm = getManager(sce);

      if (cm == null) {
         cm = getMcInjectedCacheManager(sce.getServletContext());
         setManager(sce, cm);
      }

      // If cache manager is still null, create one for REST server's own usage
      if (cm == null) {
         String cfgFile = sce.getServletContext().getInitParameter("infinispan.config");
         if (cfgFile == null) {
            cm = new DefaultCacheManager();
         } else {
            try {
               cm = new DefaultCacheManager(cfgFile);
            } catch (IOException e) {
               log.errorReadingConfigurationFile(e, cfgFile);
               cm = new DefaultCacheManager();
            }
         }
         setManager(sce, cm);
      }

      // Start defined caches to avoid issues with lazily started caches
      for (String cacheName : cm.getCacheNames())
         cm.getCache(cacheName);

      // Finally, start default cache as well
      cm.getCache();
   }

   @Override
   public void contextDestroyed(ServletContextEvent sce) {
      EmbeddedCacheManager cm = getManager(sce);
      if (cm != null) {
         cm.stop();
      }
   }

   /**
    * To avoid any hard dependencies, checking whether the cache manager is injected via the JBoss
    * MicroContainer is done using reflection.
    */
   private EmbeddedCacheManager getMcInjectedCacheManager(ServletContext servletContext) {
      try {
         boolean isDebug = log.isDebugEnabled();
         Object kernel = servletContext.getAttribute("jboss.kernel:service=Kernel");
         if (kernel != null) {
            Class<?> kernelCl = loadClass("org.jboss.kernel.Kernel");
            Object kernelReg = kernelCl.getMethod("getRegistry").invoke(kernel);
            Class<?> kernelRegCl = loadClass("org.jboss.kernel.spi.registry.KernelRegistry");
            String beanName = servletContext.getInitParameter("infinispan.cachemanager.bean");
            if (beanName == null)
               beanName = "DefaultCacheManager";

            Object kernelRegEntry = kernelRegCl.getMethod("findEntry", Object.class).invoke(kernelReg, beanName);
            if (kernelRegEntry != null) {
               Class<?> kernelRegEntryCl = loadClass("org.jboss.kernel.spi.registry.KernelRegistryEntry");
               if (isDebug)
                  log.debug("Retrieving cache manager from JBoss Microcontainer");
               return (EmbeddedCacheManager) kernelRegEntryCl.getMethod("getTarget").invoke(kernelRegEntry);
            } else {
               if (isDebug)
                  log.debug("Running within JBoss Microcontainer but cache manager bean not present");
               return null;
            }
         }
         return null;
      } catch (Exception e) {
         log.errorRetrievingCacheManagerFromMC(e);
         return null;
      }
   }

   private Class<?> loadClass(String name) throws Exception {
      return Thread.currentThread().getContextClassLoader().loadClass(name);
   }

}
