package org.infinispan.client.hotrod;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.Provider;

/**
 * Privileged actions for package org.infinispan.client.hotrod
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Scott.Stark@jboss.org
 * @since 4.2
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return action.run();
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
