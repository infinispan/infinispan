package org.infinispan.security.actions;

import java.security.PrivilegedAction;

/**
 * SetThreadContextClassLoaderAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SetThreadContextClassLoaderAction implements PrivilegedAction<ClassLoader> {

   private final ClassLoader classLoader;

   public SetThreadContextClassLoaderAction(ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   public SetThreadContextClassLoaderAction(Class<?> klass) {
      this.classLoader = klass.getClassLoader();
   }

   @Override
   public ClassLoader run() {
      ClassLoader old = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(classLoader);
      return old;
   }



}
