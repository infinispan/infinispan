package org.infinispan.test;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.mockito.Mockito;

public class MockBasicComponentRegistry implements BasicComponentRegistry {
   private ConcurrentMap<String, RunningComponentRef> components = new ConcurrentHashMap<>();

   public void registerMocks(Class<?>... componentTypes) {
      for (Class<?> componentType : componentTypes) {
         registerComponent(componentType, Mockito.mock(componentType, Mockito.RETURNS_DEEP_STUBS), false);
      }
   }

   @Override
   public <T> ComponentRef<T> getComponent(String name, Class<T> componentType) {
      return (ComponentRef<T>) components.get(name);
   }

   @Override
   public <T> ComponentRef<T> registerComponent(String componentName, T instance, boolean manageLifecycle) {
      RunningComponentRef<T> componentRef = new RunningComponentRef<T>(componentName, instance.getClass(), instance);
      components.put(componentName, componentRef);
      return componentRef;
   }

   @Override
   public void registerAlias(String aliasName, String targetComponentName, Class<?> targetComponentType) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void wireDependencies(Object target, boolean startDependencies) {
      // Do nothing
   }

   public void registerSubComponent(String ownerComponentName, String subComponentName, Object instance) {
      registerComponent(subComponentName, instance, false);
   }

   @Override
   public void addDynamicDependency(String ownerComponentName, String dependencyComponentName) {
      // Do nothing
   }

   @Override
   public void replaceComponent(String componentName, Object newInstance, boolean manageLifecycle) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void rewire() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Collection<ComponentRef<?>> getRegisteredComponents() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void stop() {
      components.clear();
   }
}
