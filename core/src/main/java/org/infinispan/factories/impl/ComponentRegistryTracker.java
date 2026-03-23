package org.infinispan.factories.impl;

import java.util.Collection;

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
    * Record a component entering the given lifecycle state.
    *
    * @param componentName The component name (registry key).
    * @param state The new lifecycle state.
    * @param path The current dependency path, or {@code null} if unavailable.
    */
   void stateChanged(String componentName, BasicComponentRegistryImpl.WrapperState state,
                     BasicComponentRegistryImpl.ComponentPath path);

   /**
    * @return All tracked component entries. Never {@code null}.
    */
   Collection<ComponentEntry> entries();

   /**
    * Clear all tracked data.
    */
   void clear();

   /**
    * Remove a single component from tracking.
    *
    * @param componentName The component name to remove.
    */
   void removeComponent(String componentName);
}
