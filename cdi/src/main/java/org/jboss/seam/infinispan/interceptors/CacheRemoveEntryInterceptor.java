package org.jboss.seam.infinispan.interceptors;

import javax.cache.interceptor.CacheRemoveEntry;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

/**
 * @author Kevin Pollet - SERLI - (kevin.pollet@serli.com)
 */
@Interceptor
@CacheRemoveEntry
public class CacheRemoveEntryInterceptor {

   @AroundInvoke
   public Object cacheRemoveEntry(InvocationContext context) throws Exception {
      return context.proceed();
   }
}
