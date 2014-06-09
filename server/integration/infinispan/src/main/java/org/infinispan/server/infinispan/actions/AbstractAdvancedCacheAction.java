package org.infinispan.server.infinispan.actions;

import org.infinispan.AdvancedCache;

import java.security.PrivilegedAction;

/**
 * AbstractAdvancedCacheAction. A helper abstract for writing {@link java.security.PrivilegedAction}s which require an {@link org.infinispan.AdvancedCache}
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
abstract class AbstractAdvancedCacheAction<T> implements PrivilegedAction<T> {
   final AdvancedCache<?, ?> cache;

   public AbstractAdvancedCacheAction(AdvancedCache<?, ?> cache) {
      this.cache = cache;
   }

}
