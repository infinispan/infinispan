package org.infinispan.commons.util;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Privileged actions for the package
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Scott.Stark@jboss.org
 * @since 4.2
 */
final class SecurityActions {

   interface SysProps {

      SysProps NON_PRIVILEGED = new SysProps() {
         @Override
         public String getProperty(final String name, final String defaultValue) {
            return System.getProperty(name, defaultValue);
         }

         @Override
         public String getProperty(final String name) {
            return System.getProperty(name);
         }
      };

      SysProps PRIVILEGED = new SysProps() {
         @Override
         public String getProperty(final String name, final String defaultValue) {
            PrivilegedAction<String> action = new PrivilegedAction<String>() {
               @Override
               public String run() {
                  return System.getProperty(name, defaultValue);
               }
            };
            return AccessController.doPrivileged(action);
         }

         @Override
         public String getProperty(final String name) {
            PrivilegedAction<String> action = new PrivilegedAction<String>() {
               @Override
               public String run() {
                  return System.getProperty(name);
               }
            };
            return AccessController.doPrivileged(action);
         }
      };

      String getProperty(String name, String defaultValue);

      String getProperty(String name);
   }

   static String getProperty(String name, String defaultValue) {
      if (System.getSecurityManager() == null)
         return SysProps.NON_PRIVILEGED.getProperty(name, defaultValue);

      return SysProps.PRIVILEGED.getProperty(name, defaultValue);
   }

   static String getProperty(String name) {
      if (System.getSecurityManager() == null)
         return SysProps.NON_PRIVILEGED.getProperty(name);

      return SysProps.PRIVILEGED.getProperty(name);
   }
}
