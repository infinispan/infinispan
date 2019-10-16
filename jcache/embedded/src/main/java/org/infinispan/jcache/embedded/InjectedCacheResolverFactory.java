package org.infinispan.jcache.embedded;


import java.lang.annotation.Annotation;

import javax.cache.annotation.CacheMethodDetails;
import javax.cache.annotation.CacheResolver;
import javax.cache.annotation.CacheResolverFactory;
import javax.cache.annotation.CacheResult;
import javax.inject.Inject;

import org.infinispan.jcache.annotation.InjectedCacheResolverQualifier;

/**
 * {@code CacheResolverFactory} implementation that looks up the cache by name in all the
 * {@code EmbeddedCacheManager} beans that exist in the CDI container.
 *
 * If a cache with the required cache name does not exist yet, it is created in the
 * {@code @Default} {@code EmbeddedCacheManager} bean.
 *
 * Example:
 * <pre>{@code
 * @CacheResult(cacheName = "bla", cacheResolverFactory = InjectedCacheResolverFactory.class)
 * public String cachedMethod() {
 *    ...
 * }
 * }</pre>
 * @author Dan Berindei
 * @since 13.0
 */
public class InjectedCacheResolverFactory implements CacheResolverFactory {
   @InjectedCacheResolverQualifier
   @Inject
   CacheResolver injectedCacheResolver;

   @Override
   public CacheResolver getCacheResolver(CacheMethodDetails<? extends Annotation> cacheMethodDetails) {
      return injectedCacheResolver;
   }

   @Override
   public CacheResolver getExceptionCacheResolver(CacheMethodDetails<CacheResult> cacheMethodDetails) {
      return injectedCacheResolver;
   }
}
