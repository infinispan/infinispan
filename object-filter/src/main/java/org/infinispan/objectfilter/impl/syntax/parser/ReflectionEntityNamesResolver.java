package org.infinispan.objectfilter.impl.syntax.parser;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ReflectionEntityNamesResolver implements EntityNameResolver {

   private final ClassLoader[] classLoaders;

   public ReflectionEntityNamesResolver(ClassLoader userClassLoader) {
      this.classLoaders = new ClassLoader[]{userClassLoader, ClassLoader.getSystemClassLoader(), Thread.currentThread().getContextClassLoader()};
   }

   @Override
   public Class<?> resolve(String entityName) {
      for (ClassLoader cl : classLoaders) {
         try {
            return cl != null ? Class.forName(entityName, true, cl) : Class.forName(entityName);
         } catch (ClassNotFoundException | NoClassDefFoundError ex) {
            // ignore
         }
      }
      return null;
   }
}
