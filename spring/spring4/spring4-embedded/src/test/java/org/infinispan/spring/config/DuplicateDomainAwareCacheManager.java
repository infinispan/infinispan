package org.infinispan.spring.config;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

/**
 *
 * @author gustavonalle
 * @since 7.0
 */
public class DuplicateDomainAwareCacheManager extends DefaultCacheManager {

   public DuplicateDomainAwareCacheManager() {
      super(new GlobalConfigurationBuilder().globalJmxStatistics().allowDuplicateDomains(true).build());
   }
}
