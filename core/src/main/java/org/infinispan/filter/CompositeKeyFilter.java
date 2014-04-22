package org.infinispan.filter;

/**
 * Allows AND-composing several filters.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class CompositeKeyFilter<K> implements KeyFilter<K> {
   private KeyFilter<? super K>[] filters;

   public CompositeKeyFilter(KeyFilter<? super K>... filters) {
      this.filters = filters;
   }

   @Override
   public boolean accept(K key) {
      for (KeyFilter<? super K> k : filters)
         if (!k.accept(key)) return false;
      return true;
   }
}
