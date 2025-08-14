package org.infinispan.factories.impl;

/**
 * Track the execution time between the initialization phases of a component.
 *
 * <p>
 * The lifecycle methods are invoked at each stage during the component initialization. It tracks the stage until the
 * component is running, and <b>does not</b> track the shutdown time.
 * </p>
 *
 * @since 16.0
 * @author José Bolina
 */
interface ComponentRegistryTracker {

   /**
    * Register the time a given component started instantiating.
    *
    * @param componentName The component name to track the time.
    * @see org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState#INSTANTIATING
    */
   void instantiating(String componentName);

   /**
    * Register the time a given component became instantiated.
    *
    * @param componentName The component name to track the time.
    * @see org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState#INSTANTIATED
    */
   void instantiated(String componentName);

   /**
    * Register the time a component became wiring.
    *
    * @param componentName The component name to track the time.
    * @see org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState#WIRING
    */
   void wiring(String componentName);

   /**
    * Register the time a component became wired.
    *
    * @param componentName The component name to track the time.
    * @see org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState#WIRED
    */
   void wired(String componentName);

   /**
    * Register the time a component became starting.
    *
    * @param componentName The component name to track the time.
    * @see org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState#STARTING
    */
   void starting(String componentName);

   /**
    * Register the time a component became started.
    *
    * @param componentName The component name to track the time.
    * @see org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState#STARTED
    */
   void started(String componentName);

   /**
    * Generate the complete report with all components.
    *
    * @return A report containing all the registered components sorted by time. <code>null</code> is tracking is not enabled.
    */
   String dump();

   /**
    * Clear all metrics collected.
    */
   void clear();

   /**
    * Removes a component from the collected metrics.
    *
    * @param componentName The component name to remove.
    */
   void removeComponent(String componentName);

   static ComponentRegistryTracker disabled() {
      return new Empty();
   }

   static ComponentRegistryTracker timeTracking(BasicComponentRegistry registry, boolean global) {
      return ComponentRegistryTimeTracker.tracker(registry, global);
   }

   final class Empty implements ComponentRegistryTracker {

      @Override
      public void instantiating(String componentName) { }

      @Override
      public void instantiated(String componentName) { }

      @Override
      public void wiring(String componentName) { }

      @Override
      public void wired(String componentName) { }

      @Override
      public void starting(String componentName) { }

      @Override
      public void started(String componentName) { }

      @Override
      public String dump() {
         return null;
      }

      @Override
      public void clear() { }

      @Override
      public void removeComponent(String componentName) { }
   }
}
