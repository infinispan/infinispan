package org.infinispan.manager.impl;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * @since 15.0
 **/
@SurvivesRestarts
public abstract class InternalCacheManager implements EmbeddedCacheManager {

   public static GlobalComponentRegistry of(EmbeddedCacheManager cacheManager) {
      return ((InternalCacheManager) cacheManager).getGlobalComponentRegistry();
   }

   protected GlobalComponentRegistry getGlobalComponentRegistry() {
      return globalComponentRegistry();
   }

   protected abstract GlobalComponentRegistry globalComponentRegistry();
}
