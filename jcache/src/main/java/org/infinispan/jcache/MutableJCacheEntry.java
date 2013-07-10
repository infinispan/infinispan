package org.infinispan.jcache;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.ReflectionUtil;

import javax.cache.Cache;

/**
 * Infinispan implementation of {@link Cache.MutableEntry} designed to
 * be passed as parameter to {@link Cache.EntryProcessor#process(javax.cache.Cache.MutableEntry, Object...)}.
 *
 * @param <K> the type of key maintained by this cache entry
 * @param <V> the type of value maintained by this cache entry
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class MutableJCacheEntry<K, V> implements Cache.MutableEntry<K, V> {

   private final AdvancedCache<K, V> cache;

   private final K key;

   private final V oldValue;

   private V value; // mutable

   private boolean removed;

   public MutableJCacheEntry(AdvancedCache<K, V> cache, K key, V value) {
      this.cache = cache;
      this.key = key;
      this.oldValue = value;
   }

   @Override
   public boolean exists() {
      if (value != null)
         return true;
      else if (!removed)
         return cache.containsKey(key);

      return false;
   }

   @Override
   public void remove() {
      removed = true;
      value = null;
   }

   @Override
   public void setValue(V value) {
      this.value = value;
      removed = false;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public V getValue() {
      if (value != null)
         return value;

      if (!removed) {
         // No new value has been set, so going to return old value. TCK
         // listener tests expect a visit event to be fired, but we don't
         // wanna change the semantics of perceived exclusive access. So,
         // call cache.get to fire the event, and return oldValue set at
         // the start in order to comply with expectations that getValue()
         // should not see newer values in cache.
         cache.get(key);
         return oldValue;
      }

      return null;
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   V getNewValue() {
      return value;
   }

   boolean isRemoved() {
      return removed;
   }

}
