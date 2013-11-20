package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;

import javax.cache.Cache;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheResolver;
import javax.cache.annotation.GeneratedCacheKey;
import javax.interceptor.InvocationContext;
import java.io.Serializable;

/**
 * Base {@link javax.cache.annotation.CachePut} interceptor implementation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public abstract class AbstractCachePutInterceptor implements Serializable {

   private final CacheResolver cacheResolver;
   private final CacheKeyInvocationContextFactory contextFactory;

   public AbstractCachePutInterceptor(CacheResolver cacheResolver, CacheKeyInvocationContextFactory contextFactory) {
      this.cacheResolver = cacheResolver;
      this.contextFactory = contextFactory;
   }

   public Object cachePut(InvocationContext invocationContext) throws Exception {
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Interception of method named '%s'", invocationContext.getMethod().getName());
      }

      final CacheKeyInvocationContext<CachePut> cacheKeyInvocationContext = contextFactory.getCacheKeyInvocationContext(invocationContext);
      final CacheKeyGenerator cacheKeyGenerator = cacheKeyInvocationContext.unwrap(CacheKeyInvocationContextImpl.class).getCacheKeyGenerator();
      final CachePut cachePut = cacheKeyInvocationContext.getCacheAnnotation();
      final GeneratedCacheKey cacheKey = cacheKeyGenerator.generateCacheKey(cacheKeyInvocationContext);
      final Cache<GeneratedCacheKey, Object> cache = cacheResolver.resolveCache(cacheKeyInvocationContext);

      final Object valueToCache = cacheKeyInvocationContext.getValueParameter().getValue();

      if (!cachePut.afterInvocation() && valueToCache != null) {
         cache.put(cacheKey, valueToCache);
         if (getLog().isTraceEnabled()) {
            getLog().tracef("Value '%s' cached in cache '%s' with key '%s' before method invocation", valueToCache, cache.getName(), cacheKey);
         }
      }

      final Object result = invocationContext.proceed();

      if (cachePut.afterInvocation() && valueToCache != null) {
         cache.put(cacheKey, valueToCache);
         if (getLog().isTraceEnabled()) {
            getLog().tracef("Value '%s' cached in cache '%s' with key '%s' after method invocation", valueToCache, cache.getName(), cacheKey);
         }
      }

      return result;
   }

   protected abstract Log getLog();

}
