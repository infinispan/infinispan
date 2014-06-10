package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * SecurityActions for the org.infinispan.client.hotrod.event package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
final class SecurityActions {
   private static final Log log = LogFactory.getLog(SecurityActions.class);

   static void setAccessible(final Method m) {
      try {
         if (System.getSecurityManager() != null) {
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
               @Override
               public Void run() {
                  m.setAccessible(true);
                  return null;
               }
            });
         } else {
            m.setAccessible(true);
         }
      } catch (Exception e) {
         log.unableToSetAccesible(m, e);
      }
   }

}
