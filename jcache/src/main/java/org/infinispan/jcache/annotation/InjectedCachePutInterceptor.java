package org.infinispan.jcache.annotation;

import org.infinispan.cdi.InfinispanExtension;
import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CachePut;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * CachePutInterceptor for environments where the cache manager is injected
 * in a managed environment, e.g. application server.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Interceptor
@CachePut
public class InjectedCachePutInterceptor extends AbstractCachePutInterceptor {

   private static final Log log = LogFactory.getLog(InjectedCachePutInterceptor.class, Log.class);

   @Inject
   public InjectedCachePutInterceptor(InjectedCacheResolver cacheResolver,
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
