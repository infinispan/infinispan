package org.infinispan.factories.scopes;

import java.lang.annotation.Annotation;

/**
 * Retrieves the declared scope of a component
 *
 * @author Manik Surtani
 * @since 4.0
 * @deprecated Since 10.0, no longer used
 */
@Deprecated
public class ScopeDetector {
   public static Scopes detectScope(Class<?> clazz) {
      Scope s = getAnnotation(clazz, Scope.class);
      return s != null ? s.value() : Scopes.getDefaultScope();
   }

   private static <A extends Annotation> A getAnnotation(Class<?> clazz, Class<A> ann) {
      while (true) {
         // first check class
         A a = clazz.getAnnotation(ann);
         if (a != null) return a;

         // check interfaces
         if (!clazz.isInterface()) {
            Class<?>[] interfaces = clazz.getInterfaces();
            for (Class<?> inter : interfaces) {
               a = getAnnotation(inter, ann);
               if (a != null) return a;
            }
         }

         // check superclasses
         Class<?> superclass = clazz.getSuperclass();
         if (superclass == null) return null; // no where else to look
         clazz = superclass;
      }
   }
}
