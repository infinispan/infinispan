package org.infinispan.globalstate;

import java.util.EnumSet;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;

/*
 * A no-op implementation for tests which mess up with initial state transfer and RPCs
 */
public class NoOpGlobalConfigurationManager implements GlobalConfigurationManager {
   @Override
   public Configuration createCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return null;
   }

   @Override
   public Configuration createCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return null;
   }

   @Override
   public void removeCache(String cacheName, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
   }

   public static void amendCacheManager(EmbeddedCacheManager cm) {
      TestingUtil.replaceComponent(cm, GlobalConfigurationManager.class, new NoOpGlobalConfigurationManager(), true);
   }
}
