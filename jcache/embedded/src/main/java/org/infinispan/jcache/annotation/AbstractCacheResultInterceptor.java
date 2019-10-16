package org.infinispan.jcache.annotation;

import java.io.Serializable;

import javax.cache.Cache;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheResolver;
import javax.cache.annotation.CacheResult;
import javax.cache.annotation.GeneratedCacheKey;
import javax.interceptor.InvocationContext;

import org.infinispan.commons.util.Util;
import org.infinispan.jcache.logging.Log;

/**
 * <p>{@link javax.cache.annotation.CacheResult} interceptor implementation. This interceptor uses the following algorithm describes in
 * JSR-107.</p>
 *
 * <p>When a method annotated with {@link javax.cache.annotation.CacheResult} is invoked the following must occur.
 * <ol>
 *    <li>Generate a key based on InvocationContext using the specified {@linkplain javax.cache.annotation.CacheKeyGenerator}.</li>
 *    <li>Use this key to look up the entry in the cache.</li>
 *    <li>If an entry is found return it as the result and do not call the annotated method.</li>
 *    <li>If no entry is found invoke the method.</li>
 *    <li>Use the result to populate the cache with this key/result pair.</li>
 * </ol>
 *
 * There is a skipGet attribute which if set to true will cause the method body to always be invoked and the return
 * value put into the cache. The cache is not checked for the key before method body invocation, skipping steps 2 and 3
 * from the list above. This can be used for annotating methods that do a cache.put() with no other consequences.</p>
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public abstract class AbstractCacheResultInterceptor implements Serializable {
   private static final long serialVersionUID = 5275055951121834315L;

   private final CacheResolver defaultCacheResolver;
   private final CacheKeyInvocationContextFactory contextFactory;

   public AbstractCacheResultInterceptor(CacheResolver defaultCacheResolver, CacheKeyInvocationContextFactory contextFactory) {
      this.defaultCacheResolver = defaultCacheResolver;
      this.contextFactory = contextFactory;
   }

   public Object cacheResult(InvocationContext invocationContext) throws Throwable {
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Interception of method '%s.%s'",
                         invocationContext.getMethod().getDeclaringClass().getName(),
                         invocationContext.getMethod().getName());
      }

      final CacheKeyInvocationContextImpl<CacheResult> cacheKeyInvocationContext = contextFactory.getCacheKeyInvocationContext(invocationContext);
      final CacheKeyGenerator cacheKeyGenerator = cacheKeyInvocationContext.getCacheKeyGenerator();
      final CacheResult cacheResult = cacheKeyInvocationContext.getCacheAnnotation();
      final GeneratedCacheKey cacheKey = cacheKeyGenerator.generateCacheKey(cacheKeyInvocationContext);
      CacheResolver cacheResolver = cacheKeyInvocationContext.getCacheResolver();
      if (cacheResolver == null) {
         cacheResolver = defaultCacheResolver;
      }
      final Cache<GeneratedCacheKey, Object> cache = cacheResolver.resolveCache(cacheKeyInvocationContext);
      CacheResolver exceptionCacheResolver = cacheKeyInvocationContext.getExceptionCacheResolver();
      Cache<GeneratedCacheKey, Throwable> exceptionCache =
         exceptionCacheResolver != null ?
         cacheResolver.resolveCache(cacheKeyInvocationContext) : null;

      Object result = null;

      if (!cacheResult.skipGet()) {
         result = cache.get(cacheKey);
         if (getLog().isTraceEnabled()) {
            getLog().tracef("Found in cache '%s' key '%s' with value '%s'",
                            cache.getName(), cacheKey, Util.toStr(result));
         }

         if (exceptionCache != null) {
            Throwable throwable = exceptionCache.get(cacheKey);
            if (throwable != null) {
               throw throwable;
            }
         }
      }

      if (result == null) {
         try {
            result = invocationContext.proceed();

            if (result != null) {
               cache.put(cacheKey, result);
               if (getLog().isTraceEnabled()) {
                  getLog().tracef("Cached return value in cache '%s' with key '%s': '%s'",
                                  cache.getName(), cacheKey, Util.toStr(result));
               }
            }
         } catch (Throwable t) {
            cacheException(cacheResult, cacheKey, exceptionCache, t);
            throw t;
         }
      }

      return result;
   }

   private void cacheException(CacheResult cacheResult, GeneratedCacheKey cacheKey,
                               Cache<GeneratedCacheKey, Throwable> exceptionCache, Throwable t) {
      if (exceptionCache != null) {
         // Default to cache everything if the include list is empty
         boolean shouldCache = cacheResult.cachedExceptions().length == 0;
         for (Class<? extends Throwable> includedException : cacheResult.cachedExceptions()) {
            if (includedException.isAssignableFrom(t.getClass())) {
               shouldCache = true;
            }
         }
         for (Class<? extends Throwable> excludedException : cacheResult.nonCachedExceptions()) {
            if (excludedException.isAssignableFrom(t.getClass())) {
               shouldCache = false;
            }
         }
         if (shouldCache) {
            exceptionCache.put(cacheKey, t);
         }
      }
   }

   protected abstract Log getLog();

}
