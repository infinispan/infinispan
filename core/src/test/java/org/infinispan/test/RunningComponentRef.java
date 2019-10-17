package org.infinispan.test;

import org.infinispan.factories.impl.ComponentRef;

/**
 * Always-started {@link ComponentRef} implementation.
 *
 * @since 9.4
 */
public class RunningComponentRef<T> implements ComponentRef<T> {
   private final String componentName;
   private final Class<?> componentType;
   private final Object componentInstance;

   public RunningComponentRef(String componentName, Class<?> componentType, T componentInstance) {
      this.componentName = componentName;
      this.componentType = componentType;
      this.componentInstance = componentInstance;
   }

   @Override
   public T running() {
      return (T) componentInstance;
   }

   @Override
   public T wired() {
      return (T) componentInstance;
   }

   @Override
   public boolean isRunning() {
      return true;
   }

   @Override
   public boolean isWired() {
      return true;
   }

   @Override
   public boolean isAlias() {
      return false;
   }

   @Override
   public String getName() {
      return componentName != null ? componentName : componentType.getName();
   }
}
