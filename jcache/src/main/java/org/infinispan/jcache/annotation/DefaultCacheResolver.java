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

      final String cacheName = cacheInvocationContext.getCacheName();

      // If cache name is empty, default cache of default cache manager is returned
      if (cacheName.trim().isEmpty()) {
         return defaultCacheManager.createCache(cacheName,
               new javax.cache.configuration.MutableConfiguration<K, V>());
      }

      for (String name : defaultCacheManager.getCacheNames()) {
         if (name.equals(cacheName))
            return defaultCacheManager.getCache(cacheName);
      }

      // If the cache has not been defined in the default cache manager or
      // in a specific one a new cache is created in the default cache manager
      // with the default configuration.
      return defaultCacheManager.createCache(cacheName,
            new javax.cache.configuration.MutableConfiguration<K, V>());
   }

}
