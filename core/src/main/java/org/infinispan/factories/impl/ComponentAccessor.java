package org.infinispan.factories.impl;

import java.util.List;

/**
 * Component lifecycle management
 *
 * @since 10.0
 * @author Dan Berindei
 */
public class ComponentAccessor<T> {
   private final String className;
   // Some modules will not have access to the Scopes enum at runtime
   private final Integer scopeOrdinal;
   private final boolean survivesRestarts;
   private final String superAccessorName;
   private final List<String> eagerDependencies;

   public ComponentAccessor(String className, Integer scopeOrdinal, boolean survivesRestarts,
                            String superAccessorName, List<String> eagerDependencies) {
      this.className = className;
      this.scopeOrdinal = scopeOrdinal;
      this.survivesRestarts = survivesRestarts;
      this.superAccessorName = superAccessorName;
      this.eagerDependencies = eagerDependencies;
   }

   final Integer getScopeOrdinal() {
      return scopeOrdinal;
   }

   final boolean getSurvivesRestarts() {
      return survivesRestarts;
   }

   /**
    * Return name of the first class with component annotations going up in the inheritance hierarchy.
    */
   final String getSuperAccessorName() {
      return superAccessorName;
   }

   final List<String> getEagerDependencies() {
      return eagerDependencies;
   }

   protected T newInstance() {
      return null;
   }

   protected void wire(T instance, WireContext context, boolean start) {
      // Do nothing
   }

   protected void start(T instance) throws Exception {
      // Do nothing
   }

   protected void stop(T instance) throws Exception {
      // Do nothing
   }

   @Override
   public String toString() {
      return "ComponentAccessor(" + className + ")";
   }

   /**
    * Temporary, for ComponentRegistry.getLocalComponent
    */
   public boolean isGlobalScope() {
      return scopeOrdinal == Scopes.GLOBAL.ordinal();
   }
}
