package org.infinispan.demos.gridfs;

import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import java.io.IOException;

/**
 * A bootstrapping startup listener which creates and holds a cache instance
 */
public class CacheManagerHolder extends HttpServlet {

   private static final Log log = LogFactory.getLog(CacheManagerHolder.class);

   private static final String CFG_PROPERTY = "infinispan.gridfs.cfg";
   private static final String DATA_CACHE_NAME_PROPERTY = "infinispan.gridfs.cache.data";
   private static final String METADATA_CACHE_NAME_PROPERTY = "infinispan.gridfs.cache.metadata";

   public static CacheManager cacheManager;
   public static String dataCacheName, metadataCacheName;

   @Override
   public void init(ServletConfig cfg) throws ServletException {
      super.init(cfg);
      String cfgFile = System.getProperty(CFG_PROPERTY, cfg.getInitParameter(CFG_PROPERTY));
      if (cfgFile == null)
         cacheManager = new DefaultCacheManager();
      else {
         try {
            cacheManager = new DefaultCacheManager(cfgFile);
         } catch (IOException e) {
            log.error("Unable to start cache manager with config file " + cfgFile + ".  Using DEFAULTS!");
            cacheManager = new DefaultCacheManager();
         }
      }

      dataCacheName = System.getProperty(DATA_CACHE_NAME_PROPERTY, cfg.getInitParameter(DATA_CACHE_NAME_PROPERTY));
      metadataCacheName = System.getProperty(METADATA_CACHE_NAME_PROPERTY, cfg.getInitParameter(METADATA_CACHE_NAME_PROPERTY));
   }
}
