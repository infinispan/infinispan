package org.infinispan.query.impl.massindex;

import org.infinispan.Cache;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;

/**
 * Delete all indexes associated with a cache.
 *
 * @author gustavonalle
 * @since 7.0
 */
public class SingleNodeIndexPurger implements IndexPurger {

   private final Cache cache;

   public SingleNodeIndexPurger(Cache cache) {
      this.cache = cache;
   }

   public void purge() {
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      queryInterceptor.purgeAllIndexes();
   }

}
