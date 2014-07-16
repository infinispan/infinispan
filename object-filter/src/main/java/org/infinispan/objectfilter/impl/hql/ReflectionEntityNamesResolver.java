package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ReflectionEntityNamesResolver implements EntityNamesResolver {

   private final ClassLoader classLoader;

   public ReflectionEntityNamesResolver(ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   @Override
   public Class<?> getClassFromName(String entityName) {
      if (classLoader != null) {
         try {
            return classLoader.loadClass(entityName);
         } catch (ClassNotFoundException e) {
            return null;
         }
      }

      try {
         return Class.forName(entityName);
      } catch (ClassNotFoundException e) {
         return null;
      }
   }
}
