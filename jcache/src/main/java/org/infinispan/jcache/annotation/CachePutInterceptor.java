package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CachePut;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * {@link javax.cache.annotation.CachePut} interceptor implementation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
@Interceptor
@CachePut
public class CachePutInterceptor extends AbstractCachePutInterceptor {

   private static final Log log = LogFactory.getLog(CachePutInterceptor.class, Log.class);

   @Inject
   public CachePutInterceptor(DefaultCacheResolver cacheResolver,
         CacheKeyInvocationContextFactory contextFactory) {
      super(cacheResolver, contextFactory);
   }

   @AroundInvoke
   public Object cachePut(InvocationContext invocationContext) throws Exception {
      return super.cachePut(invocationContext);
   }

   @Override
   protected Log getLog() {
      return log;
   }

}
