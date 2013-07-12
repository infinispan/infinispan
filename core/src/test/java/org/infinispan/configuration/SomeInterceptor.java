package org.infinispan.configuration;

import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.util.concurrent.locks.LockManager;

/**
* @author Mircea Markus
* @since 5.0
*/
public class SomeInterceptor extends BaseCustomInterceptor {
   public volatile static LockManager lm = null;

   @Override
   protected void start() {
      lm = cache.getAdvancedCache().getLockManager();
   }
}
