package org.infinispan.globalstate;

import org.infinispan.configuration.cache.Configuration;

/*
 * A no-op implementation for tests which mess up with initial state transfer and RPCs
 */
public class NoOpGlobalConfigurationManager implements GlobalConfigurationManager {
   @Override
   public Configuration createCache(String cacheName, Configuration configuration) {
      return null;
   }

   @Override
   public void removeCache(String cacheName) {
   }
}
