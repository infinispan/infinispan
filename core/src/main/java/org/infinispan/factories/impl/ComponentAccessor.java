package org.infinispan.factories.impl;

import java.util.List;

import org.infinispan.factories.scopes.Scopes;

/**
 * Component lifecycle management
 *
 * @since 10.0
 * @author Dan Berindei
 */
public class ComponentAccessor<T> {
   private final String className;
   private final Scopes scope;
   private final boolean survivesRestarts;
   private final String superAccessorName;
   private final List<String> eagerDependencies;

   public ComponentAccessor(String className, Scopes scope, boolean survivesRestarts,
                            String superAccessorName, List<String> eagerDependencies) {
      this.className = className;
      this.scope = scope;
      this.survivesRestarts = survivesRestarts;
      this.superAccessorName = superAccessorName;
      this.eagerDependencies = eagerDependencies;
   }

   protected final Scopes getScope() {
      return scope;
   }

   protected final boolean getSurvivesRestarts() {
      return survivesRestarts;
   }

   /**
    * Return name of the first class with component annotations going up in the inheritance hierarchy.
    */
   protected final String getSuperAccessorName() {
      return superAccessorName;
   }

   protected final List<String> getEagerDependencies() {
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
      return scope == Scopes.GLOBAL;
   }
}
