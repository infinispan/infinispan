package org.horizon.factories.scopes;

import org.horizon.util.ReflectionUtil;

/**
 * Retrieves the declated scope of a component
 *
 * @author Manik Surtani
 * @since 1.0
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
