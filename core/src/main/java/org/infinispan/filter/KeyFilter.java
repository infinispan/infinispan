package org.infinispan.filter;

import net.jcip.annotations.ThreadSafe;

/**
 * A filter for keys.  This class is complemented by the {@link org.infinispan.filter.KeyValueFilter} class.  This
 * class is useful for cases when it may be more beneficial to not have the values available.  This would include
 * cache loaders since many implementations it may incur additional performance costs just to resurrect the values in
 * addition to any keys.
 *
 * @author Manik Surtani
 * @since 6.0
 */
@ThreadSafe
public interface KeyFilter<K> {
   public static final KeyFilter LOAD_ALL_FILTER = new KeyFilter() {
      @Override
      public boolean accept(Object key) {
         return true;
      }
   };
   /**
    * @param key key to test
    * @return true if the given key is accepted by this filter.
    */
   boolean accept(K key);
}
