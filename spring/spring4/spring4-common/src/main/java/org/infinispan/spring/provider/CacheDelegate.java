package org.infinispan.spring.provider;

import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.util.Assert;

/**
 * Since there are 2 Cache implementation with exactly the same implementation details -
 * is it convenient to introduce common abstraction and delegate all the methods.
 * This is exactly what happens here.
 *
 * @author Sebastian Laskawiec
 */
class CacheDelegate implements Cache {

   private final org.infinispan.commons.api.BasicCache<Object, Object> nativeCache;

   /**
    * @param nativeCache underlying cache
    */
   public CacheDelegate(final org.infinispan.commons.api.BasicCache<Object, Object> nativeCache) {
      Assert.notNull(nativeCache, "A non-null Infinispan cache implementation is required");
      this.nativeCache = nativeCache;
   }

   /**
    * @see org.springframework.cache.Cache#getName()
    */
   @Override
   public String getName() {
      return this.nativeCache.getName();
   }

   /**
    * @see org.springframework.cache.Cache#getNativeCache()
    */
   @Override
   public org.infinispan.commons.api.BasicCache<?, ?> getNativeCache() {
      return this.nativeCache;
   }

   /**
    * @see org.springframework.cache.Cache#get(Object)
    */
   @Override
   public ValueWrapper get(final Object key) {
      return toValueWrapper(nativeCache.get(key));
   }

   @Override
   public <T> T get(Object key, Class<T> type) {
      Object value = nativeCache.get(key);
      if (value != null && type != null && !type.isInstance(value)) {
         throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + value);
      }
      return (T) value;
   }

   /**
    * @see org.springframework.cache.Cache#put(Object, Object)
    */
   @Override
   public void put(final Object key, final Object value) {
      this.nativeCache.put(key, value != null ? value : NullValue.NULL);
   }

   @Override
   public ValueWrapper putIfAbsent(Object key, Object value) {
      return toValueWrapper(this.nativeCache.putIfAbsent(key, value));
   }

   /**
    * @see org.springframework.cache.Cache#evict(Object)
    */
   @Override
   public void evict(final Object key) {
      this.nativeCache.remove(key);
   }

   /**
    * @see org.springframework.cache.Cache#clear()
    */
   @Override
   public void clear() {
      this.nativeCache.clear();
   }


   /**
    * @see Object#toString()
    */
   @Override
   public String toString() {
      return "InfinispanCache [nativeCache = " + this.nativeCache + "]";
   }

   private ValueWrapper toValueWrapper(Object value) {
      if (value == null) {
         return null;
      }
      if (value == NullValue.NULL) {
         return NullValue.NULL;
      }
      return new SimpleValueWrapper(value);
   }

}
