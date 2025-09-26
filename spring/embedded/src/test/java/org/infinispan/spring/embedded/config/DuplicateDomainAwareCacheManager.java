package org.infinispan.spring.embedded.config;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.manager.DefaultCacheManager;

/**
 *
 * @author gustavonalle
 * @since 7.0
 */
@SurvivesRestarts
public class DuplicateDomainAwareCacheManager extends DefaultCacheManager {

   public DuplicateDomainAwareCacheManager() {
      super(new GlobalConfigurationBuilder().build());
   }
}
