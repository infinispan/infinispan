package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;

/**
 * AbstractAdvancedCacheAction. A helper abstract for writing {@link PrivilegedAction}s which require an {@link AdvancedCache}
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
