package org.infinispan.persistence;

import org.infinispan.persistence.spi.AdvancedCacheLoader;

/**
 * Allows AND-composing several filters.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class CompositeFilter implements AdvancedCacheLoader.KeyFilter {
   private AdvancedCacheLoader.KeyFilter[] filters;

   public CompositeFilter(AdvancedCacheLoader.KeyFilter... filters) {
      this.filters = filters;
   }

   @Override
   public boolean shouldLoadKey(Object key) {
      for (AdvancedCacheLoader.KeyFilter k : filters)
         if (!k.shouldLoadKey(key)) return false;
      return true;
   }
}
