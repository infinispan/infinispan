package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.Cache;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.CacheResolver;
import javax.cache.annotation.CacheResult;
import javax.cache.annotation.GeneratedCacheKey;
import javax.interceptor.InvocationContext;
import java.io.Serializable;

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
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public abstract class AbstractCacheResultInterceptor implements Serializable {

   private static final long serialVersionUID = 5275055951121834315L;

   private final CacheResolver cacheResolver;
   private final CacheKeyInvocationContextFactory contextFactory;

   public AbstractCacheResultInterceptor(CacheResolver cacheResolver, CacheKeyInvocationContextFactory contextFactory) {
      this.cacheResolver = cacheResolver;
      this.contextFactory = contextFactory;
   }

   public Object cacheResult(InvocationContext invocationContext) throws Exception {
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Interception of method named '%s'", invocationContext.getMethod().getName());
      }

      final CacheKeyInvocationContext<CacheResult> cacheKeyInvocationContext = contextFactory.getCacheKeyInvocationContext(invocationContext);
      final CacheKeyGenerator cacheKeyGenerator = cacheKeyInvocationContext.unwrap(CacheKeyInvocationContextImpl.class).getCacheKeyGenerator();
      final CacheResult cacheResult = cacheKeyInvocationContext.getCacheAnnotation();
      final GeneratedCacheKey cacheKey = cacheKeyGenerator.generateCacheKey(cacheKeyInvocationContext);
      final Cache<GeneratedCacheKey, Object> cache = cacheResolver.resolveCache(cacheKeyInvocationContext);

      Object result = null;

      if (!cacheResult.skipGet()) {
         result = cache.get(cacheKey);
         if (getLog().isTraceEnabled()) {
            getLog().tracef("Entry with value '%s' has been found in cache '%s' with key '%s'", result, cache.getName(), cacheKey);
         }
      }

      if (result == null) {
         result = invocationContext.proceed();
         if (result != null) {
            cache.put(cacheKey, result);
            if (getLog().isTraceEnabled()) {
               getLog().tracef("Value '%s' cached in cache '%s' with key '%s'", result, cache.getName(), cacheKey);
            }
         }
      }

      return result;
   }

   protected abstract Log getLog();

}
