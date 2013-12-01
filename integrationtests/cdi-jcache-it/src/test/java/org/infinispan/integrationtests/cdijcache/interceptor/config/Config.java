package org.infinispan.integrationtests.cdijcache.interceptor.config;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Config {

   private static final Log log = LogFactory.getLog(Config.class);

   /**
    * <p>Associates the "custom" cache with the qualifier {@link Custom}.</p>
    *
    * <p>The default configuration will be used.</p>
    */
   @Custom
   @ConfigureCache("custom")
   @Produces
   @SuppressWarnings("unused")
   public Configuration customConfiguration;

   /**
    * <p>Associates the "small" cache with the qualifier {@link Small}.</p>
    *
    * <p>The default configuration will be used.</p>
    */
   @Small
   @ConfigureCache("small")
   @Produces
   @SuppressWarnings("unused")
   public Configuration smallConfiguration;

   /**
    * Associates the "small" cache with the small cache manager.
    */
   @Small
   @Produces
   @ApplicationScoped
   @SuppressWarnings("unused")
   EmbeddedCacheManager smallCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.eviction().maxEntries(4);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      log.tracef("Create small cache manager %s", cm);
      return cm;
   }

   /**
    * Stops cache manager.
    *
    * @param cacheManager to be stopped
    */
   @SuppressWarnings("unused")
   public void killCacheManager(@Disposes @Small EmbeddedCacheManager cacheManager) {
      log.tracef("Kill cache manager via dispose: %s", cacheManager);
      TestingUtil.killCacheManagers(cacheManager);
      log.tracef("Killed, cache manager status: %s", cacheManager.getStatus());
   }

}
