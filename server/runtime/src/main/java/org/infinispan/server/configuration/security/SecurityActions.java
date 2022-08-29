package org.infinispan.server.configuration.security;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.Provider;

import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.security.Security;

/**
 * @since 14.0
 **/
class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   public static Provider[] discoverSecurityProviders(ClassLoader classLoader) {
      return doPrivileged(() -> SslContextFactory.discoverSecurityProviders(classLoader));
   }
}
