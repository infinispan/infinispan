package org.infinispan.factories.scopes;

import org.infinispan.util.ReflectionUtil;

/**
 * Retrieves the declated scope of a component
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ScopeDetector {
   public static Scopes detectScope(Class clazz) {
      Scope s = ReflectionUtil.getAnnotation(clazz, Scope.class);
      if (s == null)
         return Scopes.getDefaultScope();
      else
         return s.value();
   }
}
