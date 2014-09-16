package org.infinispan.jcache.annotation;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.annotation.CacheInvocationContext;
import javax.cache.annotation.CacheResolver;
import javax.cache.spi.CachingProvider;
import javax.enterprise.context.ApplicationScoped;
import java.lang.annotation.Annotation;

import static org.infinispan.jcache.annotation.Contracts.assertNotNull;

/**
 * Default {@link javax.cache.annotation.CacheResolver} implementation for
 * standalone environments, where no Cache/CacheManagers are injected via CDI.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
@ApplicationScoped
public class DefaultCacheResolver implements CacheResolver {

   private CacheManager defaultCacheManager;

   // Created by proxy
   @SuppressWarnings("unused")
   DefaultCacheResolver() {
      CachingProvider provider = Caching.getCachingProvider();
      defaultCacheManager = provider.getCacheManager(provider.getDefaultURI(), provider.getDefaultClassLoader());
   }

   @Override
   public <K, V> Cache<K, V> resolveCache(CacheInvocationContext<? extends Annotation> cacheInvocationContext) {
      assertNotNull(cacheInvocationContext, "cacheInvocationContext parameter must not be null");
      String cacheName = cacheInvocationContext.getCacheName();
      return getOrCreateCache(cacheName);
   }

   private synchronized <K, V> Cache<K, V> getOrCreateCache(String cacheName) {
      Cache<K, V> cache = defaultCacheManager.getCache(cacheName);
      if (cache != null)
         return cache;

      return defaultCacheManager.createCache(cacheName,
            new javax.cache.configuration.MutableConfiguration<K, V>());
   }

}
