package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CacheResult;
import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * <p>{@link javax.cache.annotation.CacheResult} interceptor implementation. This interceptor uses the following algorithm describes in
 * JSR-107.</p>
 *
 * <p>When a method annotated with {@link javax.cache.annotation.CacheResult} is invoked the following must occur.
 * <ol>
 *    <li>Generate a key based on InvocationContext using the specified {@linkplain javax.cache.annotation.CacheKeyGenerator}.</li>
 *    <li>Use this key to look up the entry in the cache.</li>
 *    <li>If an entry is found return it as the result and do not call the annotated method.</li>
 *    <li>If no entry is found invoke the method.</li>
 *    <li>Use the result to populate the cache with this key/result pair.</li>
 * </ol>
 *
 * There is a skipGet attribute which if set to true will cause the method body to always be invoked and the return
 * value put into the cache. The cache is not checked for the key before method body invocation, skipping steps 2 and 3
 * from the list above. This can be used for annotating methods that do a cache.put() with no other consequences.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
@Interceptor
@CacheResult
public class CacheResultInterceptor extends AbstractCacheResultInterceptor {

   private static final long serialVersionUID = 5275055951121834315L;
   private static final Log log = LogFactory.getLog(CacheResultInterceptor.class, Log.class);

   @Inject
   public CacheResultInterceptor(DefaultCacheResolver cacheResolver,
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
