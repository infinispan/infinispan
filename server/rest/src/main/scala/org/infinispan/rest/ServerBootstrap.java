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
import org.infinispan.rest.configuration.ExtendedHeaders;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.logging.JavaLog;
import org.infinispan.util.logging.LogFactory;

/**
 * Initializes cache manager for the REST server and sets it into the servlet context.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 * @since 5.2
 */
public class ServerBootstrap implements ServletContextListener {

   /**
    * The name of an Infinispan configuration file to load
    */
   private static final String INFINISPAN_CONFIG = "infinispan.config";
   /**
    * Whether to allow returning extended metadata headers
    */
   private static final String EXTENDED_HEADERS = "extended.headers";

   private final static JavaLog log = LogFactory.getLog(ServerBootstrap.class, JavaLog.class);

   // Attributes attached to the ServletContext
   public final static String CACHE_MANAGER = "org.infinispan.rest.ServerBootstrap.CACHE_MANAGER";
   public final static String CONFIGURATION = "org.infinispan.rest.ServerBootstrap.CONFIGURATION";
   public final static String MANAGER_INSTANCE = "org.infinispan.rest.ServerBootstrap.MANAGER_INSTANCE";

   public static void setCacheManager(ServletContext ctx, EmbeddedCacheManager cacheManager) {
      ctx.setAttribute(CACHE_MANAGER, cacheManager);
      ctx.setAttribute(MANAGER_INSTANCE, new ManagerInstance(cacheManager));
   }

   public static EmbeddedCacheManager getCacheManager(ServletContext ctx) {
      return (EmbeddedCacheManager) ctx.getAttribute(CACHE_MANAGER);
   }

   public static RestServerConfiguration getConfiguration(ServletContext ctx) {
      return (RestServerConfiguration) ctx.getAttribute(CONFIGURATION);
   }

   public static void setConfiguration(ServletContext ctx, RestServerConfiguration cfg) {
      ctx.setAttribute(CONFIGURATION, cfg);
   }

   public static ManagerInstance getManagerInstance(ServletContext ctx) {
      return (ManagerInstance) ctx.getAttribute(MANAGER_INSTANCE);
   }


   @Override
   public void contextInitialized(ServletContextEvent sce) {
      ServletContext ctx = sce.getServletContext();

      // Try to obtain an externally injected CacheManager
      EmbeddedCacheManager cm = getCacheManager(ctx);

      // If cache manager is null, create one for REST server's own usage
      if (cm == null) {
         cm = createCacheManager(ctx);
      }

      // REST Server configuration
      if (getConfiguration(ctx) == null) {
         createConfiguration(ctx);
      }

      // Start defined caches to avoid issues with lazily started caches
      for (String cacheName : cm.getCacheNames())
         cm.getCache(cacheName);

      // Finally, start default cache as well
      cm.getCache();
   }

   private void createConfiguration(ServletContext ctx) {
      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      String extendedHeaders = ctx.getInitParameter(EXTENDED_HEADERS);
      if (extendedHeaders != null) {
         builder.extendedHeaders(ExtendedHeaders.valueOf(extendedHeaders));
      }
      setConfiguration(ctx, builder.build());
   }

   private EmbeddedCacheManager createCacheManager(ServletContext ctx) {
      EmbeddedCacheManager cm;
      String cfgFile = ctx.getInitParameter(INFINISPAN_CONFIG);
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
      setCacheManager(ctx, cm);
      return cm;
   }

   @Override
   public void contextDestroyed(ServletContextEvent sce) {
      EmbeddedCacheManager cm = getCacheManager(sce.getServletContext());
      if (cm != null) {
         cm.stop();
      }
   }

   private Class<?> loadClass(String name) throws Exception {
      return Thread.currentThread().getContextClassLoader().loadClass(name);
   }

}
