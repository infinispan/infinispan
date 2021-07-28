package org.infinispan.jcache.annotation;

import javax.cache.annotation.CacheResolver;
import javax.cache.annotation.CacheResult;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.jcache.embedded.InjectedCacheResolverFactory;
import org.infinispan.jcache.logging.Log;

/**
 * CacheResultInterceptor for environments where the cache manager is
 * injected in a managed environment, e.g. application server.
 *
 * @author Galder Zamarreño
 * @deprecated Since 13.0, please use {@link InjectedCacheResolverFactory} instead.
 */
@Deprecated
@Interceptor
@CacheResult
public class InjectedCacheResultInterceptor extends AbstractCacheResultInterceptor {

   private static final Log log = LogFactory.getLog(InjectedCacheResultInterceptor.class, Log.class);

   @Inject
   public InjectedCacheResultInterceptor(@InjectedCacheResolverQualifier CacheResolver cacheResolver,
                                         CacheKeyInvocationContextFactory contextFactory) {
      super(cacheResolver, contextFactory);
   }

   @AroundInvoke
   public Object cacheResult(InvocationContext invocationContext) throws Throwable {
      return super.cacheResult(invocationContext);
   }

   @Override
   protected Log getLog() {
      return log;
   }

}
