package org.infinispan.factories.impl;

import java.util.IdentityHashMap;
import java.util.Map;

import org.infinispan.manager.TestModuleRepository;
import org.infinispan.test.RunningComponentRef;

/**
 * Helper for TestingUtil.inject() and TestingUtil.invokeLifecycle()
 */
public final class TestComponentAccessors {

   public static void wire(Object target, Object... components) {
      TestWireContext wireContext = new TestWireContext(components);
      String accessorName = target.getClass().getName();
      while (accessorName != null) {
         ComponentAccessor<Object> componentAccessor =
            TestModuleRepository.defaultModuleRepository().getComponentAccessor(accessorName);
         componentAccessor.wire(target, wireContext, false);
         accessorName = componentAccessor.getSuperAccessorName();
      }
      Map<Object, Object> unmatchedComponents = new IdentityHashMap<>();
      for (Object component : components) {
         if (!wireContext.matchedComponents.containsKey(component)) {
            unmatchedComponents.put(component, component);
         }
      }
      if (!unmatchedComponents.isEmpty()) {
         throw new IllegalArgumentException("No fields match components " + unmatchedComponents.values());
      }
   }

   public static void start(Object target) throws Exception {
      String accessorName = target.getClass().getName();
      while (accessorName != null) {
         ComponentAccessor<Object> componentAccessor =
            TestModuleRepository.defaultModuleRepository().getComponentAccessor(accessorName);
         componentAccessor.start(target);
         accessorName = componentAccessor.getSuperAccessorName();
      }
   }

   public static void stop(Object target) throws Exception {
      String accessorName = target.getClass().getName();
      while (accessorName != null) {
         ComponentAccessor<Object> componentAccessor =
            TestModuleRepository.defaultModuleRepository().getComponentAccessor(accessorName);
         componentAccessor.stop(target);
         accessorName = componentAccessor.getSuperAccessorName();
      }
   }

   public static final class NamedComponent {
      private final String name;
      private final Object component;

      public NamedComponent(String name, Object component) {
         this.name = name;
         this.component = component;
      }

      @Override
      public String toString() {
         return "NamedComponent{" +
                "name='" + name + '\'' +
                ", component=" + component +
                '}';
      }
   }

   static class TestWireContext extends WireContext {
      private final Object[] components;
      private final Map<Object, Object> matchedComponents;

      public TestWireContext(Object[] components) {
         super(null);

         this.components = components;
         matchedComponents = new IdentityHashMap<>(components.length);
      }

      private <T> T findComponent(String componentName, Class<T> componentType) {
         for (Object component : components) {
            Object currentMatch = null;
            Object componentInstance = null;
            if (component instanceof NamedComponent) {
               NamedComponent nc = (NamedComponent) component;
               if (componentName.equals(nc.name)) {
                  currentMatch = nc;
                  componentInstance = nc.component;
               }
            } else {
               if (componentType.isInstance(component)) {
                  currentMatch = component;
                  componentInstance = component;
               }
            }
            if (currentMatch != null) {
               Object previousMatch = matchedComponents.put(currentMatch, currentMatch);
               if (previousMatch != null) {
                  throw new IllegalArgumentException("Ambiguous injection, dependency " + componentName +
                                                     " is matched by both " + previousMatch + " and " + component);
               }
               return (T) componentInstance;
            }
         }
         return null;
      }

      @Override
      public <T> T get(String componentName, Class<T> componentClass, boolean start) {
         return findComponent(componentName, componentClass);
      }

      @Override
      public <T, U extends T> ComponentRef<T> getLazy(String componentName, Class<U> componentClass, boolean start) {
         return new RunningComponentRef<T>(componentName, componentClass, findComponent(componentName, componentClass));
      }
   }
}
