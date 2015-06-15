package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultCacheContainer;

import java.security.PrivilegedAction;

/**
 * AbstractDefaultEmbeddedCacheManagerAction. A helper abstract for writing {@link java.security.PrivilegedAction}s
 * which require an {@link org.jboss.as.clustering.infinispan.DefaultCacheContainer}
 *
 * @author William Burns
 * @since 7.0
 */
abstract class AbstractDefaultCacheContainerAction<T> implements PrivilegedAction<T> {
   final DefaultCacheContainer cacheManager;

   public AbstractDefaultCacheContainerAction(DefaultCacheContainer cacheManager) {
      this.cacheManager = cacheManager;
   }

}
