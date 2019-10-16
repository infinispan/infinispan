package org.infinispan.jcache.annotation;

import java.io.Serializable;

import javax.cache.Cache;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheResolver;
import javax.cache.annotation.GeneratedCacheKey;
import javax.interceptor.InvocationContext;

import org.infinispan.commons.util.Util;
import org.infinispan.jcache.logging.Log;

/**
 * Base {@link javax.cache.annotation.CachePut} interceptor implementation.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public abstract class AbstractCachePutInterceptor implements Serializable {
   private final CacheResolver defaultCacheResolver;
   private final CacheKeyInvocationContextFactory contextFactory;

   public AbstractCachePutInterceptor(CacheResolver defaultCacheResolver, CacheKeyInvocationContextFactory contextFactory) {
      this.defaultCacheResolver = defaultCacheResolver;
      this.contextFactory = contextFactory;
   }

   public Object cachePut(InvocationContext invocationContext) throws Exception {
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Interception of method '%s.%s'",
                         invocationContext.getMethod().getDeclaringClass().getName(),
                         invocationContext.getMethod().getName());
      }

      final CacheKeyInvocationContextImpl<CachePut> cacheKeyInvocationContext = contextFactory.getCacheKeyInvocationContext(invocationContext);
      final CacheKeyGenerator cacheKeyGenerator = cacheKeyInvocationContext.getCacheKeyGenerator();
      final CachePut cachePut = cacheKeyInvocationContext.getCacheAnnotation();
      final GeneratedCacheKey cacheKey = cacheKeyGenerator.generateCacheKey(cacheKeyInvocationContext);
      CacheResolver cacheResolver = cacheKeyInvocationContext.getCacheResolver();
      if (cacheResolver == null) {
         cacheResolver = defaultCacheResolver;
      }
      final Cache<GeneratedCacheKey, Object> cache = cacheResolver.resolveCache(cacheKeyInvocationContext);

      final Object valueToCache = cacheKeyInvocationContext.getValueParameter().getValue();

      if (!cachePut.afterInvocation() && valueToCache != null) {
         cache.put(cacheKey, valueToCache);
         if (getLog().isTraceEnabled()) {
            getLog().tracef("Value stored before invocation in cache '%s' with key '%s': '%s'",
                            cache.getName(), cacheKey, Util.toStr(valueToCache));
         }
      }

      final Object result = invocationContext.proceed();

      if (cachePut.afterInvocation() && valueToCache != null) {
         cache.put(cacheKey, valueToCache);
         if (getLog().isTraceEnabled()) {
            getLog().tracef("Value stored after invocation in cache '%s' with key '%s': '%s'",
                            cache.getName(), cacheKey, Util.toStr(valueToCache));
         }
      }

      return result;
   }

   protected abstract Log getLog();

}
