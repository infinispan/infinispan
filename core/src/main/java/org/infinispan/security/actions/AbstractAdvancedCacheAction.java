package org.infinispan.security.actions;

import java.util.function.Supplier;

import org.infinispan.AdvancedCache;

/**
 * AbstractAdvancedCacheAction. A helper abstract for writing {@link Supplier}s which require an {@link AdvancedCache}
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
abstract class AbstractAdvancedCacheAction<T> implements Supplier<T> {
   final AdvancedCache<?, ?> cache;

   public AbstractAdvancedCacheAction(AdvancedCache<?, ?> cache) {
      this.cache = cache;
   }

}
