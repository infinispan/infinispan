package org.infinispan.demo.distexec;

import java.io.IOException;

import org.infinispan.config.Configuration;
import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.config.FluentConfiguration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.FileLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Builds CacheManager given Infinispan configuration and transport file. 
 */
public class CacheBuilder {

   private Log log = LogFactory.getLog(CacheBuilder.class);
   private EmbeddedCacheManager cacheManager;

   public CacheBuilder(String ispnConfigFile, String transportConfigFile) throws IOException {
      String defaultTransportConfigFile = "tcp.xml";
      String transportFile = findConfigFile(transportConfigFile, defaultTransportConfigFile);

      if (transportFile == null)
         throw new IllegalArgumentException("Could not find " + transportConfigFile + " nor "
                  + defaultTransportConfigFile + " configuration files. Check your classpath!");

      String configFile = findConfigFile(ispnConfigFile, null);
      boolean useDeclarativeConfig = configFile != null;
      if (useDeclarativeConfig) {
         InfinispanConfiguration c = InfinispanConfiguration.newInfinispanConfiguration(configFile);
         GlobalConfiguration gc = c.parseGlobalConfiguration();
         log.infof("Using %s and %s configuration files to create CacheManager ", configFile, transportFile);
         gc.fluent().transport().addProperty("configurationFile", transportFile);
         cacheManager = new DefaultCacheManager(gc, c.parseDefaultConfiguration());
      } else {
         GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
         log.infof("Using %s transport configuration file to create CacheManager ", transportFile);

         gc.fluent().transport().addProperty("configurationFile", transportFile);
         Configuration cfg = new Configuration();
         FluentConfiguration c = cfg.fluent();
         c.clustering().mode(CacheMode.DIST_SYNC).stateRetrieval().fetchInMemoryState(false);
         c.clustering().sync().replTimeout(30000L);
         c.transaction().syncCommitPhase(true).syncRollbackPhase(true);
         cacheManager = new DefaultCacheManager(gc, cfg);
      }

      Runtime.getRuntime().addShutdownHook(new ShutdownHook(cacheManager));
      cacheManager.start();
   }

   public EmbeddedCacheManager getCacheManager() {
      return this.cacheManager;
   }

   private String findConfigFile(String configFile, String defaultConfigFile) {
      String selectedConfigFile = null;
      FileLookup fl = new FileLookup();
      if (configFile != null && fl.lookupFile(configFile) != null) {
         selectedConfigFile = configFile;
      } else if (defaultConfigFile != null && fl.lookupFile(defaultConfigFile) != null) {
         selectedConfigFile = defaultConfigFile;
      }
      return selectedConfigFile;
   }
}

class ShutdownHook extends Thread {
   private CacheContainer container;

   /**
    * @param cacheContainer
    */
   public ShutdownHook(CacheContainer cacheContainer) {
      container = cacheContainer;
   }

   public void run() {
      System.out.println("Shutting down Cache Manager");
      container.stop();
   }
}
