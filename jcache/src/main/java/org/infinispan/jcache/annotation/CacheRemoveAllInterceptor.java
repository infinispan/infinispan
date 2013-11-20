package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CacheRemoveAll;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * <p>{@link javax.cache.annotation.CacheRemoveAll} interceptor implementation. This interceptor uses the following algorithm describes in
 * JSR-107.</p>
 *
 * <p>The interceptor that intercepts method annotated with {@code @CacheRemoveAll} must do the following, remove all
 * entries associated with the cache. The removeAll occurs after the method body is executed. This can be overridden by
 * specifying a afterInvocation attribute value of false. If afterInvocation is true and the annotated method throws an
 * exception, the removeAll will not happen.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Interceptor
@CacheRemoveAll
public class CacheRemoveAllInterceptor extends AbstractCacheRemoveAllInterceptor {

   private static final Log log = LogFactory.getLog(CacheRemoveAllInterceptor.class, Log.class);

   @Inject
   public CacheRemoveAllInterceptor(DefaultCacheResolver cacheResolver,
         CacheKeyInvocationContextFactory contextFactory) {
      super(cacheResolver, contextFactory);
   }

   @AroundInvoke
   public Object cacheRemoveAll(InvocationContext invocationContext) throws Exception {
      return super.cacheRemoveAll(invocationContext);
   }

   @Override
   protected Log getLog() {
      return log;
   }

}
