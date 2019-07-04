package org.infinispan.factories.scopes;

/**
 * Retrieves the declared scope of a component
 *
 * @author Manik Surtani
 * @since 4.0
 * @deprecated Since 10.0, annotations are no longer available at runtime
 */
@Deprecated
public class ScopeDetector {
   public static Scopes detectScope(Class<?> clazz) {
      return Scopes.getDefaultScope();
   }
}
