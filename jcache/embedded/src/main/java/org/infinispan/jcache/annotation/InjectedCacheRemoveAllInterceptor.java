package org.infinispan.jcache.annotation;

import javax.cache.annotation.CacheRemoveAll;
import javax.cache.annotation.CacheResolver;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.jcache.embedded.InjectedCacheResolverFactory;
import org.infinispan.jcache.logging.Log;

/**
 * CacheRemoveAllInterceptor for environments where the cache manager is
 * injected in a managed environment, e.g. application server.
 *
 * @author Galder Zamarreño
 * @deprecated Since 13.0, please use {@link InjectedCacheResolverFactory} instead.
 */
@Deprecated
@Interceptor
@CacheRemoveAll
public class InjectedCacheRemoveAllInterceptor extends AbstractCacheRemoveAllInterceptor {

   private static final Log log = LogFactory.getLog(InjectedCacheRemoveAllInterceptor.class, Log.class);

   @Inject
   public InjectedCacheRemoveAllInterceptor(@InjectedCacheResolverQualifier CacheResolver cacheResolver,
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
