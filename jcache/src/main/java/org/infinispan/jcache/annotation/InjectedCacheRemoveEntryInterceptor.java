package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CacheRemove;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * CacheRemoveEntryInterceptor for environments where the cache manager is
 * injected in a managed environment, e.g. application server.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Interceptor
@CacheRemove
public class InjectedCacheRemoveEntryInterceptor extends AbstractCacheRemoveEntryInterceptor {

   private static final Log log = LogFactory.getLog(InjectedCacheRemoveEntryInterceptor.class, Log.class);

   @Inject
   public InjectedCacheRemoveEntryInterceptor(InjectedCacheResolver cacheResolver,
         CacheKeyInvocationContextFactory contextFactory) {
      super(cacheResolver, contextFactory);
   }

   @Override
   @AroundInvoke
   public Object cacheRemoveEntry(InvocationContext invocationContext) throws Exception {
      return super.cacheRemoveEntry(invocationContext);
   }

   @Override
   protected Log getLog() {
      return log;
   }

}
