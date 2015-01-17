package org.infinispan.jcache;

import javax.cache.processor.MutableEntry;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.ReflectionUtil;

/**
 * Infinispan implementation of {@link MutableEntry} designed to
 * be passed as parameter to {@link javax.cache.processor.EntryProcessor#process(javax.cache.processor.MutableEntry, Object...)}.
 *
 * @param <K> the type of key maintained by this cache entry
 * @param <V> the type of value maintained by this cache entry
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class MutableJCacheEntry<K, V> implements MutableEntry<K, V> {

   private final BasicCache<K, V> cache;
   private final BasicCache<K, V> cacheWithoutStats;

   private final K key;

   private final V oldValue;

   private V value; // mutable

   private Operation operation;

   public MutableJCacheEntry(BasicCache<K, V> cache, BasicCache<K, V> cacheWithoutStats, K key, V value) {
      this.cache = cache;
      this.cacheWithoutStats = cacheWithoutStats;
      this.key = key;
      this.oldValue = value;
      this.operation = Operation.NONE;
   }

   @Override
   public boolean exists() {
      if (value != null)
         return true;
      else if (!operation.isRemoved())
         return cacheWithoutStats.containsKey(key);

      return false;
   }

   @Override
   public void remove() {
      operation = value != null ? Operation.NONE : Operation.REMOVE;
      value = null;
   }

   @Override
   public void setValue(V value) {
      this.value = value;
      operation = Operation.UPDATE;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public V getValue() {
      if (value != null)
         return value;

      if (!operation.isRemoved()) {

         if (oldValue != null) {
            operation = Operation.ACCESS;
            return oldValue;
         } else {
            // If not updated or removed, and old entry is null, do a read-through
            return cache.get(key);
         }
      }

      return null;
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   // TODO: was initially package-level
   public V getNewValue() {
      return value;
   }

   // TODO: was initially package-level
   public Operation getOperation() {
      return operation;
   }

   public enum Operation {
      NONE, ACCESS, REMOVE, UPDATE;

      boolean isRemoved() {
         return this == REMOVE;
      }
   }

}
