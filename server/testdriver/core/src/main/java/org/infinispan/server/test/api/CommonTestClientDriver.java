package org.infinispan.server.test.api;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.configuration.cache.CacheMode;

public interface CommonTestClientDriver<T extends CommonTestClientDriver<T>> {
   static StringConfiguration forCacheMode(CacheMode mode) {
      return switch (mode) {
         case LOCAL -> DefaultTemplate.LOCAL.getConfiguration();
         case DIST_ASYNC -> DefaultTemplate.DIST_ASYNC.getConfiguration();
         case DIST_SYNC -> DefaultTemplate.DIST_SYNC.getConfiguration();
         case REPL_ASYNC -> DefaultTemplate.REPL_ASYNC.getConfiguration();
         case REPL_SYNC -> DefaultTemplate.REPL_SYNC.getConfiguration();
         default -> throw new IllegalArgumentException(mode.toString());
      };
   }

   T withServerConfiguration(org.infinispan.configuration.cache.ConfigurationBuilder serverConfiguration);

   T withServerConfiguration(StringConfiguration configuration);

   T withCacheMode(CacheMode mode);
}
