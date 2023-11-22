package org.infinispan.jcache.annotation;

import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheResolver;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.jcache.embedded.InjectedCacheResolverFactory;
import org.infinispan.jcache.logging.Log;

/**
 * CachePutInterceptor for environments where the cache manager is injected
 * in a managed environment, e.g. application server.
 *
 * @author Galder Zamarreño
 * @deprecated Since 13.0, please use {@link InjectedCacheResolverFactory} instead.
 */
@Deprecated(forRemoval = true)
@Interceptor
@CachePut
public class InjectedCachePutInterceptor extends AbstractCachePutInterceptor {

   private static final Log log = LogFactory.getLog(InjectedCachePutInterceptor.class, Log.class);

   @Inject
   public InjectedCachePutInterceptor(@InjectedCacheResolverQualifier CacheResolver cacheResolver,
                                      CacheKeyInvocationContextFactory contextFactory) {
      super(cacheResolver, contextFactory);
   }

   @Override
   @AroundInvoke
   public Object cachePut(InvocationContext invocationContext) throws Exception {
      return super.cachePut(invocationContext);
   }

   @Override
   protected Log getLog() {
      return log;
   }

}
