package org.infinispan.server.security;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.Provider;

import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.server.security package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other {@link
 * java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant <tristan@infinispan.org>
 * @since 11.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static void addSecurityProvider(Provider provider) {
      doPrivileged(() -> {
               if (java.security.Security.getProvider(provider.getName()) == null) {
                  java.security.Security.insertProviderAt(provider, 1);
               }
               return null;
            }
      );
   }
}
