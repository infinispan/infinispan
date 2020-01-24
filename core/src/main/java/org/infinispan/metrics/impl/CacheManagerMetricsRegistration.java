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

   @Override
   protected String initNodeName() {
      String nodeName = globalConfig.transport().nodeName();
      if (nodeName == null || nodeName.isEmpty()) {
         //TODO [anistor] ensure unique node name is set in all tests and also in real life usage
         nodeName = java.util.UUID.randomUUID().toString();
         //throw new CacheConfigurationException("Node name must be specified if metrics are enabled.");
      }
      return NameUtils.filterIllegalChars(nodeName);
   }
}
