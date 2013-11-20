package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CacheRemove;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * <p>{@link javax.cache.annotation.CacheRemove} interceptor implementation.This interceptor uses the following algorithm describes in
 * JSR-107.</p>
 *
 * <p>The interceptor that intercepts method annotated with {@code @CacheRemoveEntry} must do the following, generate a
 * key based on InvocationContext using the specified {@link javax.cache.annotation.CacheKeyGenerator}, use this key to remove the entry in the
 * cache. The remove occurs after the method body is executed. This can be overridden by specifying a afterInvocation
 * attribute value of false. If afterInvocation is true and the annotated method throws an exception the remove will not
 * happen.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
@Interceptor
@CacheRemove
public class CacheRemoveEntryInterceptor extends AbstractCacheRemoveEntryInterceptor {

   private static final Log log = LogFactory.getLog(CacheRemoveEntryInterceptor.class, Log.class);

   @Inject
   public CacheRemoveEntryInterceptor(DefaultCacheResolver cacheResolver,
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
