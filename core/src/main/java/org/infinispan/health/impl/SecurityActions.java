package org.infinispan.health.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheDistributionManagerAction;

class SecurityActions {

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static DistributionManager getDistributionManager(final Cache<?, ?> cache) {
      GetCacheDistributionManagerAction action = new GetCacheDistributionManagerAction(cache.getAdvancedCache());
      return doPrivileged(action);
   }

}
