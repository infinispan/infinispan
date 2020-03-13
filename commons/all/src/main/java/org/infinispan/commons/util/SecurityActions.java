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
            return AccessController.doPrivileged((PrivilegedAction<String>) () -> System.getProperty(name, defaultValue));
         }

         @Override
         public String getProperty(final String name) {
            return AccessController.doPrivileged((PrivilegedAction<String>) () -> System.getProperty(name));
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

   private static ClassLoader getOSGiClassLoader() {
      // Make loading class optional
      try {
         Class<?> osgiClassLoader = Class.forName("org.infinispan.commons.util.OsgiClassLoader");
         return (ClassLoader) osgiClassLoader.getMethod("getInstance", null).invoke(null);
      } catch (ClassNotFoundException e) {
         // fall back option - it can't hurt if we scan ctx class loader 2 times.
         return Thread.currentThread().getContextClassLoader();
      } catch (Exception e) {
         throw new RuntimeException("Unable to call getInstance on OsgiClassLoader", e);
      }
   }

   static ClassLoader[] getOSGIContextClassLoaders(ClassLoader appClassLoader) {
      return doPrivileged(() -> new ClassLoader[]{
            appClassLoader,   // User defined classes
            getOSGiClassLoader(), // OSGi bundle context needs to be on top of TCCL, system CL, etc.
            Util.class.getClassLoader(),           // Infinispan classes (not always on TCCL [modular env])
            ClassLoader.getSystemClassLoader(),    // Used when load time instrumentation is in effect
            Thread.currentThread().getContextClassLoader() //Used by jboss-as stuff
      });
   }
}
