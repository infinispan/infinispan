package org.infinispan.jcache;

import org.infinispan.commons.util.ReflectionUtil;

import javax.cache.Cache.Entry;

/**
 * Infinispan implementation of {@link javax.cache.Cache.Entry<K, V>}.
 *
 * @param <K> the type of key maintained by this cache entry
 * @param <V> the type of value maintained by this cache entry
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class JCacheEntry<K, V> implements Entry<K, V> {

   protected final K key;

   protected final V value;

   public JCacheEntry(K key, V value) {
      this.key = key;
      this.value = value;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public V getValue() {
      return value;
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

}
