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

         @Override
         public String getEnv(String name) {
            return System.getenv(name);
         }
      };

      SysProps PRIVILEGED = new SysProps() {
         @Override
         public String getProperty(final String name, final String defaultValue) {
            return AccessController.doPrivileged((PrivilegedAction<String>) () -> System.getProperty(name, defaultValue));
         }

         @Override
         public String getProperty(final String name) {
            return AccessController.doPrivileged((PrivilegedAction<String>) () -> System.getProperty(name));
         }

         @Override
         public String getEnv(String name) {
            return AccessController.doPrivileged((PrivilegedAction<String>) () -> System.getenv(name));
         }
      };

      String getProperty(String name, String defaultValue);

      String getProperty(String name);

      String getEnv(String name);
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

   static String getEnv(String name) {
      if (System.getSecurityManager() == null)
         return SysProps.NON_PRIVILEGED.getEnv(name);

      return SysProps.PRIVILEGED.getEnv(name);
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return action.run();
      }
   }

   static ClassLoader[] getClassLoaders(ClassLoader appClassLoader) {
      return doPrivileged(() -> new ClassLoader[]{
            appClassLoader,   // User defined classes
            Util.class.getClassLoader(),           // Infinispan classes (not always on TCCL [modular env])
            ClassLoader.getSystemClassLoader(),    // Used when load time instrumentation is in effect
            Thread.currentThread().getContextClassLoader() //Used by jboss-as stuff
      });
   }
}
