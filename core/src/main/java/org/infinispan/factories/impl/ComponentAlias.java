package org.infinispan.factories.impl;

public final class ComponentAlias {
   private final String componentName;

   public static ComponentAlias of(Class<?> componentType) {
      return of(componentType.getName());
   }

   public static ComponentAlias of(String componentName) {
      return new ComponentAlias(componentName);
   }

   private ComponentAlias(String componentName) {
      this.componentName = componentName;
   }

   public String getComponentName() {
      return componentName;
   }
}
