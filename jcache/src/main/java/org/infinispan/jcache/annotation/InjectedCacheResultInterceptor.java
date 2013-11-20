package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CacheResult;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * CacheResultInterceptor for environments where the cache manager is
 * injected in a managed environment, e.g. application server.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Interceptor
@CacheResult
public class InjectedCacheResultInterceptor extends AbstractCacheResultInterceptor {

   private static final Log log = LogFactory.getLog(InjectedCacheResultInterceptor.class, Log.class);

   @Inject
   public InjectedCacheResultInterceptor(InjectedCacheResolver cacheResolver,
         CacheKeyInvocationContextFactory contextFactory) {
      super(cacheResolver, contextFactory);
   }

   @AroundInvoke
   public Object cacheResult(InvocationContext invocationContext) throws Exception {
      return super.cacheResult(invocationContext);
   }

   @Override
   protected Log getLog() {
      return log;
   }

}
