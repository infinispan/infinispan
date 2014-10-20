package org.infinispan.spring.provider;

import org.infinispan.client.hotrod.RemoteCache;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link org.springframework.cache.Cache <code>Cache</code>} implementation that delegates to a
 * {@link org.infinispan.client.hotrod.RemoteCache <code>org.infinispan.client.hotrod.RemoteCache</code>} instance supplied at construction
 * time.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author <a href="mailto:marius.bogoevici@gmail.com">Marius Bogoevici</a>
 *
 */
public class SpringRemoteCache implements Cache {

   private final RemoteCache<Object, Object> nativeCache;

   /**
    * @param nativeCache the underlying cache
    */
   public SpringRemoteCache(final RemoteCache<Object, Object> nativeCache) {
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
   public RemoteCache<?, ?> getNativeCache() {
      return this.nativeCache;
   }

   /**
    * @see org.springframework.cache.Cache#get(java.lang.Object)
    */
   @Override
   public ValueWrapper get(final Object key) {
      Object v = nativeCache.get(key);
      if (v == null) {
         return null;
      }
      if (v == NullValue.NULL) {
         return NullValue.NULL;
      }
      return new SimpleValueWrapper(v);
   }

   /**
    * @see org.springframework.cache.Cache#put(java.lang.Object, java.lang.Object)
    */
   @Override
   public void put(final Object key, final Object value) {
      this.nativeCache.put(key, value != null ? value : NullValue.NULL);
   }

   /**
    * @see org.springframework.cache.Cache#evict(java.lang.Object)
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
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "InfinispanCache [nativeCache = " + this.nativeCache + "]";
   }

}
