package org.infinispan.filter;

/**
 * This is a KeyFilter that utilizes the given {@link org.infinispan.filter.KeyValueFilter} to determine if to
 * filter the key.  Note this filter will be passed null for both the value and metadata on every pass.
 *
 * @author wburns
 * @since 7.0
 */
public class KeyValueFilterAsKeyFilter<K> implements KeyFilter<K> {
   private final KeyValueFilter<? super K, ?> filter;
   public KeyValueFilterAsKeyFilter(KeyValueFilter<? super K, ?> filter) {
      this.filter = filter;
   }
   @Override
   public boolean accept(K key) {
      return filter.accept(key, null, null);
   }
}
