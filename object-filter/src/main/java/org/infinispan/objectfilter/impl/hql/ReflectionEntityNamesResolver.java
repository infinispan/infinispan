package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ReflectionEntityNamesResolver implements EntityNamesResolver {

   private final ClassLoader[] classLoaders;

   public ReflectionEntityNamesResolver(ClassLoader userClassLoader) {
      this.classLoaders = new ClassLoader[]{userClassLoader, ClassLoader.getSystemClassLoader(), Thread.currentThread().getContextClassLoader()};
   }

   @Override
   public Class<?> getClassFromName(String entityName) {
      for (ClassLoader cl : classLoaders) {
         try {
            if (cl != null) {
               return Class.forName(entityName, true, cl);
            } else {
               return Class.forName(entityName);
            }
         } catch (ClassNotFoundException ex) {
            // ignore
         } catch (NoClassDefFoundError er) {
            // ignore
         }
      }
      return null;
   }
}
