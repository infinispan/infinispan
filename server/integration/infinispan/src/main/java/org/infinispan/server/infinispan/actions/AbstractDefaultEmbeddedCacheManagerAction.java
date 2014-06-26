package org.infinispan.server.infinispan.actions;

import org.jboss.as.clustering.infinispan.DefaultEmbeddedCacheManager;

import java.security.PrivilegedAction;

/**
 * AbstractDefaultEmbeddedCacheManagerAction. A helper abstract for writing {@link java.security.PrivilegedAction}s
 * which require an {@link org.jboss.as.clustering.infinispan.DefaultEmbeddedCacheManager}
 *
 * @author William Burns
 * @since 7.0
 */
abstract class AbstractDefaultEmbeddedCacheManagerAction<T> implements PrivilegedAction<T> {
   final DefaultEmbeddedCacheManager cacheManager;

   public AbstractDefaultEmbeddedCacheManagerAction(DefaultEmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

}
