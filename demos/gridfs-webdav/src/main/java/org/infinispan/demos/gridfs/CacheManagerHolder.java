package org.infinispan.demos.gridfs;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A bootstrapping startup listener which creates and holds a cache instance
 */
public class CacheManagerHolder implements ServletContextListener {

   private static final Log log = LogFactory.getLog(CacheManagerHolder.class);

   private static final String CFG_PROPERTY = "infinispan.config";
   private static final String DATA_CACHE_NAME_PROPERTY = "infinispan.gridfs.cache.data";
   private static final String METADATA_CACHE_NAME_PROPERTY = "infinispan.gridfs.cache.metadata";

   public static CacheContainer cacheContainer;
   public static String dataCacheName, metadataCacheName;

   @Override
   public void contextInitialized(ServletContextEvent servletContextEvent) {
      ServletContext servletContext = servletContextEvent.getServletContext();

      String cfgFile = servletContext.getInitParameter(CFG_PROPERTY);
      if (cfgFile == null)
         cacheContainer = new DefaultCacheManager();
      else {
         try {
            cacheContainer = new DefaultCacheManager(cfgFile);
         } catch (IOException e) {
            log.error("Unable to start cache manager with config file " + cfgFile + ".  Using DEFAULTS!");
            cacheContainer = new DefaultCacheManager();
         }
      }

      dataCacheName = servletContext.getInitParameter(DATA_CACHE_NAME_PROPERTY);
      metadataCacheName = servletContext.getInitParameter(METADATA_CACHE_NAME_PROPERTY);
   }

   @Override
   public void contextDestroyed(ServletContextEvent servletContextEvent) {
      if(cacheContainer != null){
         cacheContainer.stop();
      }
   }
}
