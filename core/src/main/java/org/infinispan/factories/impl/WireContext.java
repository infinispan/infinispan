package org.infinispan.factories.impl;

/**
 * Used by {@link ComponentAccessor} implementations for retrieving a component based on name and class.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class WireContext {
   private final BasicComponentRegistryImpl registry;

   /**
    * Package-private
    */
   WireContext(BasicComponentRegistryImpl registry) {
      this.registry = registry;
   }

   public <T> T get(String componentName, Class<T> componentClass, boolean start) {
      ComponentRef<T> componentRef = registry.getComponent0(componentName, componentClass, true);
      if (componentRef == null) {
         return registry.throwDependencyNotFound(componentName);
      }
      return start ? componentRef.running() : componentRef.wired();
   }

   public <T, U extends T> ComponentRef<T> getLazy(String componentName, Class<U> componentClass, boolean start) {
      ComponentRef<T> componentRef = registry.getComponent0(componentName, componentClass, false);
      if (componentRef == null) {
         registry.throwDependencyNotFound(componentName);
      }
      return componentRef;
   }
}
