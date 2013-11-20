package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CacheRemoveAll;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * CacheRemoveAllInterceptor for environments where the cache manager is
 * injected in a managed environment, e.g. application server.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Interceptor
@CacheRemoveAll
public class InjectedCacheRemoveAllInterceptor extends AbstractCacheRemoveAllInterceptor {

   private static final Log log = LogFactory.getLog(InjectedCacheRemoveAllInterceptor.class, Log.class);

   @Inject
   public InjectedCacheRemoveAllInterceptor(InjectedCacheResolver cacheResolver,
         CacheKeyInvocationContextFactory contextFactory) {
      super(cacheResolver, contextFactory);
   }

   @Override
   @AroundInvoke
   public Object cacheRemoveAll(InvocationContext invocationContext) throws Exception {
      return super.cacheRemoveAll(invocationContext);
   }

   @Override
   protected Log getLog() {
      return log;
   }

}
