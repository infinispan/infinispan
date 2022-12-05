package org.infinispan.security.actions;

import java.util.function.Supplier;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * AbstractEmbeddedCacheManagerAction. A helper abstract for writing security-sensitive {@link Supplier}s which require an {@link EmbeddedCacheManager}
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
abstract class AbstractEmbeddedCacheManagerAction<T> implements Supplier<T> {
   final EmbeddedCacheManager cacheManager;

   public AbstractEmbeddedCacheManagerAction(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

}
