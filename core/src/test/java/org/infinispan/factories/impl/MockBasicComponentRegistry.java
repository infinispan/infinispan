package org.infinispan.factories.impl;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.test.RunningComponentRef;
import org.mockito.Mockito;

public class MockBasicComponentRegistry implements BasicComponentRegistry {
   private final ConcurrentMap<String, RunningComponentRef> components = new ConcurrentHashMap<>();

   public void registerMock(String componentName, Class<?> componentType) {
      registerComponent(componentName, Mockito.mock(componentType, Mockito.RETURNS_DEEP_STUBS), false);
   }

   public void registerMocks(Class<?>... componentTypes) {
      for (Class<?> componentType : componentTypes) {
         registerComponent(componentType, Mockito.mock(componentType, Mockito.RETURNS_DEEP_STUBS), false);
      }
   }

   @Override
   public <T, U extends T> ComponentRef<T> getComponent(String name, Class<U> componentType) {
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
   public MBeanMetadata getMBeanMetadata(String className) {
      return null;
   }

   @Override
   public boolean hasComponentAccessor(String componentClassName) {
      return false;
   }

   @Override
   public void stop() {
      components.clear();
   }

   @Override
   public <T> ComponentRef<T> lazyGetComponent(Class<T> componentType) {
      return (ComponentRef<T>) components.get(componentType.getName());
   }
}
