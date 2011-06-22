package org.jboss.seam.infinispan.interceptors;

import javax.interceptor.AroundInvoke;
import javax.interceptor.InvocationContext;

/**
 * @author Kevin Pollet - SERLI - (kevin.pollet@serli.com)
 */
//@Interceptor
//@CacheRemoveAll -- Not available on type
public class CacheRemoveAllInterceptor {

   @AroundInvoke
   public Object cacheRemoveAll(InvocationContext context) throws Exception {
      return context.proceed();
   }
}
