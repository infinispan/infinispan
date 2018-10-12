package org.infinispan.jcache.annotation;

import javax.cache.annotation.CachePut;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.jcache.logging.Log;

/**
 * {@link javax.cache.annotation.CachePut} interceptor implementation.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
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
