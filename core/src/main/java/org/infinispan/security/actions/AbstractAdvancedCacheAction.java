package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.security.SecureCache;

/**
 * AbstractAdvancedCacheAction. A helper abstract for writing {@link PrivilegedAction}s which require an {@link AdvancedCache}
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
abstract class AbstractAdvancedCacheAction<T> implements ContextAwarePrivilegedAction<T> {
   final AdvancedCache<?, ?> cache;

   public AbstractAdvancedCacheAction(AdvancedCache<?, ?> cache) {
      this.cache = cache;
   }

   @Override
   public boolean contextRequiresSecurity() {
      return AbstractDelegatingCache.unwrapCache(cache) instanceof SecureCache;
   }
}
