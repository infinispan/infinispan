package org.infinispan.metrics.impl;

import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public final class CacheManagerMetricsRegistration extends AbstractMetricsRegistration {

   @Override
   protected String initNamePrefix() {
      return "cache_manager_" + NameUtils.filterIllegalChars(globalConfig.cacheManagerName());
   }
}
