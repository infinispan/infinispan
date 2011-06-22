package org.jboss.seam.infinispan.interceptors;

import javax.cache.interceptor.CacheResult;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * @author Kevin Pollet - SERLI - (kevin.pollet@serli.com)
 */
@Interceptor
@CacheResult
public class CacheResultInterceptor {

   @AroundInvoke
   public Object cacheResult(InvocationContext context) throws Exception {
      return context.proceed();
   }
}
