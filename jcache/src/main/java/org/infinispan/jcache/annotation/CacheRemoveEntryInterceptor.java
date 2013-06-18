package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.Cache;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.CacheRemoveEntry;
import javax.cache.annotation.CacheResolver;
import javax.cache.annotation.GeneratedCacheKey;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.io.Serializable;

/**
 * <p>{@link javax.cache.annotation.CacheRemoveEntry} interceptor implementation.This interceptor uses the following algorithm describes in
 * JSR-107.</p>
 *
 * <p>The interceptor that intercepts method annotated with {@code @CacheRemoveEntry} must do the following, generate a
 * key based on InvocationContext using the specified {@link javax.cache.annotation.CacheKeyGenerator}, use this key to remove the entry in the
 * cache. The remove occurs after the method body is executed. This can be overridden by specifying a afterInvocation
 * attribute value of false. If afterInvocation is true and the annotated method throws an exception the remove will not
 * happen.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
@Interceptor
@CacheRemoveEntry
public class CacheRemoveEntryInterceptor implements Serializable {

   private static final long serialVersionUID = -9079291622309963969L;
   private static final Log log = LogFactory.getLog(CacheRemoveEntryInterceptor.class, Log.class);


   private final CacheResolver cacheResolver;
   private final CacheKeyInvocationContextFactory contextFactory;

   @Inject
   public CacheRemoveEntryInterceptor(CacheResolver cacheResolver, CacheKeyInvocationContextFactory contextFactory) {
      this.cacheResolver = cacheResolver;
      this.contextFactory = contextFactory;
   }

   @AroundInvoke
   public Object cacheRemoveEntry(InvocationContext invocationContext) throws Exception {
      if (log.isTraceEnabled()) {
         log.tracef("Interception of method named '%s'", invocationContext.getMethod().getName());
      }

      final CacheKeyInvocationContext<CacheRemoveEntry> cacheKeyInvocationContext = contextFactory.getCacheKeyInvocationContext(invocationContext);
      final CacheKeyGenerator cacheKeyGenerator = cacheKeyInvocationContext.unwrap(CacheKeyInvocationContextImpl.class).getCacheKeyGenerator();
      final Cache<GeneratedCacheKey, Object> cache = cacheResolver.resolveCache(cacheKeyInvocationContext);
      final CacheRemoveEntry cacheRemoveEntry = cacheKeyInvocationContext.getCacheAnnotation();
      final GeneratedCacheKey cacheKey = cacheKeyGenerator.generateCacheKey(cacheKeyInvocationContext);

      if (!cacheRemoveEntry.afterInvocation()) {
         cache.remove(cacheKey);
         if (log.isTraceEnabled()) {
            log.tracef("Remove entry with key '%s' in cache '%s' before method invocation", cacheKey, cache.getName());
         }
      }

      final Object result = invocationContext.proceed();

      if (cacheRemoveEntry.afterInvocation()) {
         cache.remove(cacheKey);
         if (log.isTraceEnabled()) {
            log.tracef("Remove entry with key '%s' in cache '%s' after method invocation", cacheKey, cache.getName());
         }
      }

      return result;
   }
}
