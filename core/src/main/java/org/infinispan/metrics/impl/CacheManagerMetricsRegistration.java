package org.infinispan.metrics.impl;

import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Creates and registers metrics for all components from a cache manager's global component registry.
 *
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public final class CacheManagerMetricsRegistration extends AbstractMetricsRegistration {

   @Override
   protected String initNamePrefix() {
      String prefix = globalConfig.metrics().namesAsTags() ?
            "" : "cache_manager_" + NameUtils.filterIllegalChars(globalConfig.cacheManagerName()) + '_';
      String globalPrefix = globalConfig.metrics().prefix();
      return globalPrefix != null && !globalPrefix.isEmpty() ? globalPrefix + '_' + prefix : prefix;
   }
}
