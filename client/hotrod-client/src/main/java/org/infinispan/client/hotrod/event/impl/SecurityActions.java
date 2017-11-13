package org.infinispan.client.hotrod.event.impl;

import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * SecurityActions for the org.infinispan.client.hotrod.event.impl package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
final class SecurityActions {
   private static final Log log = LogFactory.getLog(SecurityActions.class);

   private SecurityActions() {
   }

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
