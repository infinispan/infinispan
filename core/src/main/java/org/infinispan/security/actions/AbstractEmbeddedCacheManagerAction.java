package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * AbstractEmbeddedCacheManagerAction. A helper abstract for writing {@link PrivilegedAction}s which require an {@link EmbeddedCacheManager}
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
abstract class AbstractEmbeddedCacheManagerAction<T> implements PrivilegedAction<T> {
   final EmbeddedCacheManager cacheManager;

   public AbstractEmbeddedCacheManagerAction(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

}
