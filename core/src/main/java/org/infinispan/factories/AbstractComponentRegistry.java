package org.infinispan.factories;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistryImpl;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.ModuleRepository;
import org.infinispan.util.logging.Log;

/**
 * A registry where components which have been created are stored.  Components are stored as singletons, registered
 * under a specific name.
  * Components can be retrieved from the registry using {@link #getComponent(Class)}.
  * Components can be registered using {@link #registerComponent(Object, Class)}, which will cause any dependencies to be
 * wired in as well.  Components that need to be created as a result of wiring will be done using {@link
 * #getOrCreateComponent(Class)}, which will look up the default factory for the component type (factories annotated
 * with the appropriate {@link DefaultFactoryFor} annotation.
  * Default factories are treated as components too and will need to be wired before being used.
  * The registry can exist in one of several states, as defined by the {@link org.infinispan.lifecycle.ComponentStatus}
 * enumeration. In terms of the cache, state changes in the following manner: <ul> <li>INSTANTIATED - when first
 * constructed</li> <li>CONSTRUCTED - when created using the DefaultCacheFactory</li> <li>STARTED - when {@link
 * org.infinispan.Cache#start()} is called</li> <li>STOPPED - when {@link org.infinispan.Cache#stop()} is called</li>
 * </ul>
  * Cache configuration can only be changed and will only be re-injected if the cache is not in the {@link
 * org.infinispan.lifecycle.ComponentStatus#RUNNING} state.
 *
 * Thread Safety: instances of {@link GlobalComponentRegistry} can be concurrently updated so all
 * the write operations are serialized through class intrinsic lock.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 * @deprecated Since 9.4, please use {@link BasicComponentRegistry} instead.
 */
@Deprecated(forRemoval=true, since = "9.4")
public abstract class AbstractComponentRegistry implements Lifecycle {

   final ModuleRepository moduleRepository;
   final BasicComponentRegistry basicComponentRegistry;
   protected volatile ComponentStatus state = ComponentStatus.INSTANTIATED;

   AbstractComponentRegistry(ModuleRepository moduleRepository,
                             boolean isGlobal, BasicComponentRegistry nextBasicComponentRegistry) {
      this.moduleRepository = moduleRepository;
      this.basicComponentRegistry = new BasicComponentRegistryImpl(moduleRepository, isGlobal, nextBasicComponentRegistry);
   }

   /**
    * Retrieves the state of the registry
    *
    * @return state of the registry
    */
   public ComponentStatus getStatus() {
      return state;
   }

   protected abstract ClassLoader getClassLoader();

   protected abstract Log getLog();

   /**
    * Wires an object instance with dependencies annotated with the {@link Inject} annotation, creating more components
    * as needed based on the Configuration passed in if these additional components don't exist in the {@link
    * ComponentRegistry}.  Strictly for components that don't otherwise live in the registry and have a lifecycle, such
    * as Commands.
    *
    * @param target object to wire
    * @throws CacheConfigurationException if there is a problem wiring the instance
    */
   public void wireDependencies(Object target) throws CacheConfigurationException {
      basicComponentRegistry.wireDependencies(target, true);
   }

   /**
    * Wires an object instance with dependencies annotated with the {@link Inject} annotation, creating more components
    * as needed based on the Configuration passed in if these additional components don't exist in the {@link
    * ComponentRegistry}.  Strictly for components that don't otherwise live in the registry and have a lifecycle, such
    * as Commands.
    *
    * @param target object to wire
    * @param startDependencies whether to start injected components (if not already started)
    * @throws CacheConfigurationException if there is a problem wiring the instance
    */
   public void wireDependencies(Object target, boolean startDependencies) throws CacheConfigurationException {
      basicComponentRegistry.wireDependencies(target, startDependencies);
   }

   /**
    * Registers a component in the registry under the given type, and injects any dependencies needed.
    *
    * Note: Until 9.4, if a component of this type already existed, it was overwritten.
    *
    * @param component component to register
    * @param type      type of component
    * @throws org.infinispan.commons.CacheConfigurationException If a component is already registered with that
    *         name, or if a dependency cannot be resolved
    */
   public final void registerComponent(Object component, Class<?> type) {
      registerComponent(component, type.getName(), type == component.getClass());
   }

   public final void registerComponent(Object component, String name) {
      registerComponent(component, name, name.equals(component.getClass().getName()));
   }

   public final void registerComponent(Object component, String name, boolean nameIsFQCN) {
      registerComponentInternal(component, name, nameIsFQCN);
   }

   protected final void registerNonVolatileComponent(Object component, String name) {
      registerComponentInternal(component, name, false);
   }

   protected void registerComponentInternal(Object component, String name, boolean nameIsFQCN) {
      ComponentRef<Object> ref = basicComponentRegistry.registerComponent(name, component, true);
      if (state == ComponentStatus.INITIALIZING || state == ComponentStatus.RUNNING) {
         // Force the component to start if the registry is already running
         ref.running();
      }
   }

   /**
    * Retrieves a component if one exists, and if not, attempts to find a factory capable of constructing the component
    * (factories annotated with the {@link DefaultFactoryFor} annotation that is capable of creating the component
    * class).
        * If an instance needs to be constructed, dependencies are then automatically wired into the instance, based on
    * methods on the component type annotated with {@link Inject}.
        * Summing it up, component retrieval happens in the following order:<br /> 1.  Look for a component that has already
    * been created and registered. 2.  Look for an appropriate component that exists in the {@link Configuration} that
    * may be injected from an external system. 3.  Look for a class definition passed in to the {@link Configuration} -
    * such as an EvictionPolicy implementation 4.  Attempt to create it by looking for an appropriate factory (annotated
    * with {@link DefaultFactoryFor})
        *
    * @param componentClass type of component to be retrieved.  Should not be null.
    * @return a fully wired component instance, or null if one cannot be found or constructed.
    * @throws CacheConfigurationException if there is a problem with constructing or wiring the instance.
    */
   protected <T> T getOrCreateComponent(Class<T> componentClass) {
      return getComponent(componentClass, componentClass.getName());
   }

   protected <T> T getOrCreateComponent(Class<T> componentClass, String name) {
      return getComponent(componentClass, name);
   }

   protected <T> T getOrCreateComponent(Class<T> componentClass, String name, boolean nameIsFQCN) {
      return getComponent(componentClass, name);
   }

   /**
    * Retrieves a component of a specified type from the registry, or null if it cannot be found.
    *
    * @param type type to find
    * @return component, or null
    */
   public <T> T getComponent(Class<T> type) {
      String className = type.getName();
      return getComponent(type, className);
   }

   @SuppressWarnings("unchecked")
   public <T> T getComponent(String componentClassName) {
      return (T) getComponent(componentClassName, componentClassName, true);
   }

   @SuppressWarnings("unchecked")
   public <T> T getComponent(String componentClassName, String name) {
      return (T) getComponent(componentClassName, name, false);
   }

   public <T> T getComponent(Class<T> componentClass, String name) {
      ComponentRef<T> component = basicComponentRegistry.getComponent(name, componentClass);
      return component != null ? component.wired() : null;
   }

   @SuppressWarnings("unchecked")
   public <T> T getComponent(String componentClassName, String name, boolean nameIsFQCN) {
      return (T) getComponent(Object.class, name);
   }

   public <T> Optional<T> getOptionalComponent(Class<T> type) {
      return Optional.ofNullable(getComponent(type));
   }

   /**
    * Rewires components.  Used to rewire components in the CR if a cache has been stopped (moved to state TERMINATED),
    * which would (almost) empty the registry of components.  Rewiring will re-inject all dependencies so that the cache
    * can be started again.
        */
   public void rewire() {
      basicComponentRegistry.rewire();
   }

   // ------------------------------ START: Publicly available lifecycle methods -----------------------------
   //   These methods perform a check for appropriate transition and then delegate to similarly named internal methods.

   /**
    * This starts the components in the registry, connecting to channels, starting service threads, etc.  If the component is
    * not in the {@link org.infinispan.lifecycle.ComponentStatus#INITIALIZING} state, it will be initialized first.
    */
   @Override
   public void start() {
      synchronized (this) {
         try {
            while (state == ComponentStatus.INITIALIZING) {
               wait();
            }
            if (state != ComponentStatus.INSTANTIATED) {
               return;
            }

            state = ComponentStatus.INITIALIZING;
         } catch (InterruptedException e) {
            throw new CacheException("Interrupted waiting for the component registry to start");
         }
      }

      try {
         preStart();

         internalStart();
         CompletionStage<Void> cs = delayStart();
         if (cs == null || CompletionStages.isCompletedSuccessfully(cs)) {
            updateStatusRunning();
            postStart();
         } else {
            cs.whenComplete((ignore, t) -> {
               if (t != null) {
                  componentFailed(t);
               } else {
                  updateStatusRunning();
                  postStart();
               }
            });
         }
      } catch (Throwable t) {
         componentFailed(t);
      }
   }

   private synchronized void updateStatusRunning() {
      if (state == ComponentStatus.INITIALIZING) {
         state = ComponentStatus.RUNNING;
         notifyAll();
      }
   }

   private void componentFailed(Throwable t) {
      synchronized (this) {
         state = ComponentStatus.FAILED;
         notifyAll();
      }

      Log.CONFIG.startFailure(getName(), t);
      try {
         stop();
      } catch (Throwable t1) {
         t.addSuppressed(t1);
      }

      handleLifecycleTransitionFailure(t);
   }

   protected abstract String getName();

   protected abstract void preStart();

   protected abstract void postStart();

   protected abstract CompletionStage<Void> delayStart();

   /**
    * Stops the component and sets its status to {@link org.infinispan.lifecycle.ComponentStatus#TERMINATED} once it
    * is done.  If the component is not in the {@link org.infinispan.lifecycle.ComponentStatus#RUNNING} state, this is a
    * no-op.
    */
   @Override
   public final void stop() {
      // Trying to stop() from FAILED is valid, but may not work
      boolean failed;

      synchronized (this) {
         try {
            while (state == ComponentStatus.STOPPING) {
               wait();
            }
            if (!state.stopAllowed()) {
               getLog().debugf("Ignoring call to stop() as current state is %s", state);
               return;
            }

            failed = state == ComponentStatus.FAILED;
            state = ComponentStatus.STOPPING;
         } catch (InterruptedException e) {
            throw new CacheException("Interrupted waiting for the component registry to stop");
         }
      }

      preStop();

      try {
         internalStop();

         postStop();
      } catch (Throwable t) {
         if (failed) {
            getLog().failedToCallStopAfterFailure(t);
         } else {
            handleLifecycleTransitionFailure(t);
         }
      } finally {
         synchronized (this) {
            state = ComponentStatus.TERMINATED;
            notifyAll();
         }
      }
   }

   protected abstract void postStop();

   protected abstract void preStop();

   /**
    * Sets the cacheStatus to FAILED and re-throws the problem as one of the declared types. Converts any
    * non-RuntimeException Exception to CacheException.
    *
    * @param t throwable thrown during failure
    */
   private void handleLifecycleTransitionFailure(Throwable t) {
      if (t.getCause() != null && t.getCause() instanceof CacheConfigurationException)
         throw (CacheConfigurationException) t.getCause();
      else if (t.getCause() != null && t.getCause() instanceof InvocationTargetException && t.getCause().getCause() != null && t.getCause().getCause() instanceof CacheConfigurationException)
         throw (CacheConfigurationException) t.getCause().getCause();
      else if (t instanceof CacheException)
         throw (CacheException) t;
      else if (t instanceof RuntimeException)
         throw (RuntimeException) t;
      else if (t instanceof Error)
         throw (Error) t;
      else
         throw new CacheException(t);
   }

   private void internalStart() throws CacheException, IllegalArgumentException {
      // Start all the components. The order doesn't matter, as starting one component starts its dependencies as well
      Collection<ComponentRef<?>> components = basicComponentRegistry.getRegisteredComponents();
      for (ComponentRef<?> component : components) {
         component.running();
      }

      addShutdownHook();
   }

   protected void addShutdownHook() {
      // no op.  Override if needed.
   }

   protected void removeShutdownHook() {
      // no op.  Override if needed.
   }

   /**
    * Actual stop
    */
   private void internalStop() {
      removeShutdownHook();

      basicComponentRegistry.stop();
   }

   public abstract TimeService getTimeService();
}
