package org.infinispan.cli.interpreter.session;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheConfigurationAction;

/**
 * SecurityActions for package org.infinispan.cli.interpreter
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(action);
      } else {
         return Security.doPrivileged(action);
      }
   }

   static Configuration getCacheConfiguration(final AdvancedCache<?, ?> cache) {
      GetCacheConfigurationAction action = new GetCacheConfigurationAction(cache);
      return doPrivileged(action);
   }
   
   interface SetThreadContextClassLoaderAction {

      ClassLoader setThreadContextClassLoader(Class cl);

      ClassLoader setThreadContextClassLoader(ClassLoader cl);

      SetThreadContextClassLoaderAction NON_PRIVILEGED = new SetThreadContextClassLoaderAction() {
         @Override
         public ClassLoader setThreadContextClassLoader(Class cl) {
            ClassLoader old = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(cl.getClassLoader());
            return old;
         }

         @Override
         public ClassLoader setThreadContextClassLoader(ClassLoader cl) {
            ClassLoader old = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(cl);
            return old;
         }
      };

      SetThreadContextClassLoaderAction PRIVILEGED = new SetThreadContextClassLoaderAction() {

         @Override
         public ClassLoader setThreadContextClassLoader(final Class cl) {
            return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
               @Override
               public ClassLoader run() {
                  ClassLoader old = Thread.currentThread().getContextClassLoader();
                  Thread.currentThread().setContextClassLoader(cl.getClassLoader());
                  return old;
               }
            });
         }

         @Override
         public ClassLoader setThreadContextClassLoader(final ClassLoader cl) {
            return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
               @Override
               public ClassLoader run() {
                  ClassLoader old = Thread.currentThread().getContextClassLoader();
                  Thread.currentThread().setContextClassLoader(cl);
                  return old;
               }
            });
         }
      };
   }

   public static ClassLoader setThreadContextClassLoader(Class cl) {
      if (System.getSecurityManager() == null) {
         return SetThreadContextClassLoaderAction.NON_PRIVILEGED.setThreadContextClassLoader(cl);
      } else {
         return SetThreadContextClassLoaderAction.PRIVILEGED.setThreadContextClassLoader(cl);
      }
   }

   public static ClassLoader setThreadContextClassLoader(ClassLoader cl) {
      if (System.getSecurityManager() == null) {
         return SetThreadContextClassLoaderAction.NON_PRIVILEGED.setThreadContextClassLoader(cl);
      } else {
         return SetThreadContextClassLoaderAction.PRIVILEGED.setThreadContextClassLoader(cl);
      }
   }
}
