package org.infinispan.spring.provider;

import org.springframework.cache.Cache;

/**
 * <p>
 * A {@link org.springframework.cache.Cache <code>Cache</code>} implementation that delegates to a
 * {@link org.infinispan.Cache <code>org.infinispan.Cache</code>} instance supplied at construction
 * time.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author <a href="mailto:marius.bogoevici@gmail.com">Marius Bogoevici</a>
 *
 */
public class SpringCache implements Cache {

   private final CacheDelegate cacheImplementation;

   /**
    * @param nativeCache underlying cache
    */
   public SpringCache(final org.infinispan.commons.api.BasicCache<Object, Object> nativeCache) {
      cacheImplementation = new CacheDelegate(nativeCache);
   }

   /**
    * @see org.springframework.cache.Cache#getName()
    */
   @Override
   public String getName() {
      return this.cacheImplementation.getName();
   }

   /**
    * @see org.springframework.cache.Cache#getNativeCache()
    */
   @Override
   public org.infinispan.commons.api.BasicCache<?, ?> getNativeCache() {
      return this.cacheImplementation.getNativeCache();
   }

   /**
    * @see org.springframework.cache.Cache#get(java.lang.Object)
    */
   @Override
   public ValueWrapper get(final Object key) {
      return cacheImplementation.get(key);
   }

   @Override
   public <T> T get(Object key, Class<T> type) {
      return cacheImplementation.get(key, type);
   }

   /**
    * @see org.springframework.cache.Cache#put(java.lang.Object, java.lang.Object)
    */
   @Override
   public void put(final Object key, final Object value) {
      this.cacheImplementation.put(key, value);
   }

   @Override
   public ValueWrapper putIfAbsent(Object key, Object value) {
      return cacheImplementation.putIfAbsent(key, value);
   }

   /**
    * @see org.springframework.cache.Cache#evict(java.lang.Object)
    */
   @Override
   public void evict(final Object key) {
      this.cacheImplementation.evict(key);
   }

   /**
    * @see org.springframework.cache.Cache#clear()
    */
   @Override
   public void clear() {
      this.cacheImplementation.clear();
   }


   /**
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "InfinispanCache [nativeCache = " + this.cacheImplementation.getNativeCache() + "]";
   }
}
