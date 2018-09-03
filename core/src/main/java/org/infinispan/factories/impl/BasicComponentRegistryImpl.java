package org.infinispan.factories.impl;

import static org.infinispan.commons.util.ReflectionUtil.invokeAccessibly;
import static org.infinispan.commons.util.ReflectionUtil.setAccessibly;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.jcip.annotations.GuardedBy;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.components.ComponentMetadata;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Dan Berindei
 * @since 8.2
 */
public class BasicComponentRegistryImpl implements BasicComponentRegistry {
   private static final Log log = LogFactory.getLog(BasicComponentRegistryImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ClassLoader classLoader;
   private final ComponentMetadataRepo metadataRepo;
   private final Scopes scope;
   private final BasicComponentRegistry next;

   private final Lock lock = new ReentrantLock();
   private final Condition condition = lock.newCondition();
   // The map and can be used without the lock, but changes to the wrapper state need the lock
   private final ConcurrentMap<String, ComponentWrapper> components = new ConcurrentHashMap<>();
   @GuardedBy("lock")
   private final List<String> startedComponents = new ArrayList<>();
   @GuardedBy("lock")
   private final Map<Thread, ComponentPath> mutatorThreads;
   // Needs to lock for updates but not for reads
   private volatile ComponentStatus status;

   public BasicComponentRegistryImpl(ClassLoader classLoader, ComponentMetadataRepo metadataRepo,
                                     Scopes scope, BasicComponentRegistry next) {
      this.classLoader = classLoader;
      this.metadataRepo = metadataRepo;
      this.scope = scope;
      this.next = next;
      this.status = ComponentStatus.RUNNING;
      this.mutatorThreads = new HashMap<>();

      // No way to look up the next scope's BasicComponentRegistry, but that's not a problem
      registerComponent(BasicComponentRegistry.class, this, false);
   }

   @Override
   public <T> ComponentRef<T> getComponent(String name, Class<T> componentType) {
      return getComponent0(name, componentType, true);
   }

   public <T> ComponentRef<T> getComponent0(String name, Class<T> componentType, boolean needInstance) {
      ComponentWrapper wrapper = components.get(name);
      if (wrapper != null && (wrapper.isAtLeast(WrapperState.WIRED) || !needInstance)) {
         // The wrapper already exists, return it even if the instance was not wired or even created yet
         return wrapper;
      }

      if (wrapper == null) {
         // The wrapper doesn't yet exist in the current scope
         // Try to find/construct the component in the next scope
         if (next != null) {
            ComponentRef nextScopeRef = next.getComponent(name, componentType);
            if (nextScopeRef != null) {
               return nextScopeRef;
            }
         }
      }

      ComponentFactory factory = findFactory(name);
      if (wrapper == null) {
         if (factory == null) {
            // Return null so the previous scope can have a go
            // It's ok to return null if another thread is registering the component concurrently
            return null;
         }

         // Create the wrapper in the current scope
         wrapper = registerWrapper(name, true);
      }

      if (needInstance) {
         instantiateWrapper(wrapper, factory);
         wireWrapper(wrapper);
      }
      return wrapper;
   }

   private ComponentWrapper registerWrapper(String name, boolean manageLifecycle) {
      ComponentWrapper wrapper = new ComponentWrapper(this, name, manageLifecycle);
      lock.lock();
      try {
         if (status != ComponentStatus.RUNNING) {
            throw new IllegalLifecycleStateException("Cannot register components while the registry is not running");
         }
         ComponentWrapper existing = components.putIfAbsent(wrapper.name, wrapper);
         return existing != null ? existing : wrapper;
      } finally {
         lock.unlock();
      }
   }

   void instantiateWrapper(ComponentWrapper wrapper, ComponentFactory factory) {
      String name = wrapper.name;
      if (!prepareWrapperChange(wrapper, WrapperState.EMPTY, WrapperState.INSTANTIATING)) {
         // Someone else has started instantiating and wiring the component, wait for them to finish
         awaitWrapperState(wrapper, WrapperState.INSTANTIATED);
         return;
      }

      Object instance;
      try {
         instance = factory.construct(name);
      } catch (Throwable t) {
         commitWrapperStateChange(wrapper, WrapperState.INSTANTIATING, WrapperState.FAILED);
         throw new CacheConfigurationException(
            "Failed to construct component " + name + ", path " + getCurrentComponentPath(), t);
      }
      if (instance instanceof ComponentAlias) {
         // Create the target component and point this wrapper to it
         ComponentAlias alias = (ComponentAlias) instance;
         commitWrapperAliasChange(wrapper, alias, null, WrapperState.INSTANTIATING, WrapperState.INSTANTIATED);
         return;
      }
      ComponentMetadata metadata = getMetadataForComponent(instance);
      if (metadata != null && metadata.getScope() != null && metadata.getScope() != scope) {
         throw new CacheConfigurationException(
            "Component " + wrapper.name + " has scope " + metadata.getScope() + " but its factory is " + scope);
      }
      commitWrapperInstanceChange(wrapper, instance, metadata, WrapperState.INSTANTIATING, WrapperState.INSTANTIATED);
   }

   void wireWrapper(ComponentWrapper wrapper) {
      if (!prepareWrapperChange(wrapper, WrapperState.INSTANTIATED, WrapperState.WIRING)) {
         // Someone else has started wiring the component, wait for them to finish
         awaitWrapperState(wrapper, WrapperState.WIRED);
         return;
      }

      if (wrapper.instance instanceof ComponentAlias) {
         ComponentAlias alias = (ComponentAlias) wrapper.instance;
         String aliasTargetName = alias.getComponentName();
         ComponentRef<Object> targetRef = getComponent(aliasTargetName, Object.class);
         if (targetRef == null) {
            throw new CacheConfigurationException(
               "Alias " + wrapper.name + " target component is missing: " + aliasTargetName);
         }
         targetRef.wired();
         commitWrapperAliasChange(wrapper, alias, targetRef, WrapperState.WIRING, WrapperState.WIRED);
      } else {
         try {
            performInjection(wrapper.instance, wrapper.metadata, false);
         } catch (Throwable t) {
            commitWrapperStateChange(wrapper, WrapperState.WIRING, WrapperState.FAILED);
            throw t;
         }

         WrapperState wiredState = wrapper.metadata != null ? WrapperState.WIRED : WrapperState.STARTED;
         commitWrapperStateChange(wrapper, WrapperState.WIRING, wiredState);
      }
   }

   @Override
   public void wireDependencies(Object target, boolean startDependencies) {
      ComponentMetadata metadata = metadataRepo.getComponentMetadata(target.getClass());
      performInjection(target, metadata, startDependencies);
   }

   private <T> ComponentFactory findFactory(String name) {
      String factoryName = metadataRepo.findFactoryForComponent(name);
      if (factoryName == null) {
         // Cannot find factory in this scope, getComponent will return null
         return null;
      }

      ComponentRef<ComponentFactory> factoryRef = getComponent(factoryName, ComponentFactory.class);
      if (factoryRef == null) {
         factoryRef = tryAutoInstantiation(factoryName);
      }
      return factoryRef != null ? factoryRef.running() : null;
   }

   private void commitWrapperStateChange(ComponentWrapper wrapper, WrapperState expectedState, WrapperState newState) {
      commitWrapperChange(wrapper, wrapper.instance, wrapper.metadata, wrapper.aliasTarget, expectedState, newState);
   }

   private void commitWrapperInstanceChange(ComponentWrapper wrapper, Object instance, ComponentMetadata metadata,
                                            WrapperState expectedState, WrapperState newState) {
      commitWrapperChange(wrapper, instance, metadata, null, expectedState, newState);
   }

   private void commitWrapperAliasChange(ComponentWrapper wrapper, ComponentAlias alias, ComponentRef<?> targetRef,
                                         WrapperState expectedState, WrapperState newState) {
      commitWrapperChange(wrapper, alias, null, targetRef, expectedState, newState);
   }

   private void commitWrapperChange(ComponentWrapper wrapper, Object instance, ComponentMetadata metadata,
                                    ComponentRef<?> aliasTargetRef,
                                    WrapperState expectedState, WrapperState newState) {
      lock.lock();
      try {
         if (wrapper.state != expectedState) {
            throw new IllegalLifecycleStateException(
               "Component " + wrapper.name + " has wrong status: " + wrapper.state + ", expected: " + expectedState);
         }
         wrapper.instance = instance;
         wrapper.metadata = metadata;
         wrapper.aliasTarget = aliasTargetRef;
         wrapper.state = newState;
         ComponentPath currentPath = mutatorThreads.get(Thread.currentThread());
         if (currentPath.next != null) {
            mutatorThreads.put(Thread.currentThread(), currentPath.next);
         } else {
            mutatorThreads.remove(Thread.currentThread());
         }
         if (trace)
            log.tracef("Changed status of " + wrapper.name + " to " + wrapper.state);
         condition.signalAll();
      } finally {
         lock.unlock();
      }
   }

   private ComponentRef<ComponentFactory> tryAutoInstantiation(String factoryName) {
      Class<? extends ComponentFactory> factoryClass = Util.loadClass(factoryName, classLoader);

      ComponentMetadata metadata = metadataRepo.getComponentMetadata(factoryClass);
      if (metadata != null && metadata.getScope() != null && metadata.getScope() != this.scope) {
         // Allow the factory to be auto-instantiated in the proper scope
         return null;
      }

      if (!(AutoInstantiableFactory.class.isAssignableFrom(factoryClass))) {
         return null;
      }

      ComponentFactory factoryFactory = new DefaultFactoryFactory(factoryClass);

      // Register, instantiate, and wire the factory (skipping steps already performed by other threads)
      ComponentWrapper wrapper = registerWrapper(factoryName, true);
      instantiateWrapper(wrapper, factoryFactory);
      wireWrapper(wrapper);
      return wrapper;
   }

   private void performInjection(Object target, ComponentMetadata metadata, boolean startDependencies) {
      try {
         if (metadata == null)
            return;

         for (ComponentMetadata.InjectFieldMetadata injectFieldMetadata : metadata.getInjectFields()) {
            setInjectionField(target, injectFieldMetadata, startDependencies);
         }
         for (ComponentMetadata.InjectMethodMetadata injectMethodMetadata : metadata.getInjectMethods()) {
            invokeInjectionMethod(target, injectMethodMetadata, startDependencies);
         }
      } catch (IllegalLifecycleStateException | CacheConfigurationException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheConfigurationException(
            "Unable to inject dependencies for component class " + target.getClass().getName() + ", path " +
            getCurrentComponentPath(), e);
      }
   }

   private void invokeInjectionMethod(Object target, ComponentMetadata.InjectMethodMetadata injectMethodMetadata,
                                      boolean startDependencies) {
      Class<?>[] parameterComponentTypes = injectMethodMetadata.getParameterClasses();
      Object[] params = new Object[parameterComponentTypes.length];
      if (trace)
         log.tracef("Injecting dependencies for method %s.%s", target.getClass().getName(), injectMethodMetadata);
      for (int i = 0; i < parameterComponentTypes.length; i++) {
         String name = injectMethodMetadata.getDependencyName(i);
         boolean parameterLazy = injectMethodMetadata.getParameterLazy(i);
         Object value = getDependency(name, parameterComponentTypes[i], parameterLazy, startDependencies);
         params[i] = value;
      }
      if (System.getSecurityManager() == null) {
         invokeAccessibly(target, injectMethodMetadata.getMethod(), params);
      } else {
         AccessController.doPrivileged(
            (PrivilegedAction<Object>) () -> invokeAccessibly(target, injectMethodMetadata.getMethod(), params));
      }
   }

   private void setInjectionField(Object target, ComponentMetadata.InjectFieldMetadata injectFieldMetadata,
                                  boolean startDependencies) {
      String name = injectFieldMetadata.getDependencyName();
      boolean lazy = injectFieldMetadata.isLazy();
      Class<?> componentType = injectFieldMetadata.getComponentClass();
      Object value = getDependency(name, componentType, lazy, startDependencies);
      if (System.getSecurityManager() == null) {
         setAccessibly(target, injectFieldMetadata.getField(), value);
      } else {
         AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            setAccessibly(target, injectFieldMetadata.getField(), value);
            return null;
         });
      }
   }

   private Object getDependency(String name, Class<?> componentType, boolean lazy, boolean startDependencies) {
      ComponentRef<?> componentRef = getComponent0(name, componentType, !lazy);
      if (componentRef == null) {
         throw new CacheConfigurationException(
            "Unable to construct dependency " + name + " in scope " + scope + " for " + getCurrentComponentPath());
      }
      Object value;
      if (lazy) {
         // Never start the target component for ComponentRef<T> fields
         value = componentRef;
      } else {
         value = startDependencies ? componentRef.running() : componentRef.wired();
      }
      return value;
   }

   @Override
   public <T> ComponentRef<T> registerComponent(String componentName, T instance, boolean manageLifecycle) {
      ComponentMetadata metadata = getMetadataForComponent(instance);
      Class<?> componentClass = instance != null ? instance.getClass() : null;
      // A missing scope declaration is interpreted as any scope
      if (metadata != null && metadata.getScope() != null && metadata.getScope() != scope) {
         throw new CacheConfigurationException(
            "Wrong registration scope " + scope + " for component class " + componentClass);
      }

      // Try to register the wrapper, but it may have been created as a lazy dependency of another component already
      ComponentWrapper wrapper = registerWrapper(componentName, manageLifecycle);
      if (!prepareWrapperChange(wrapper, WrapperState.EMPTY, WrapperState.INSTANTIATING)) {
         throw new CacheConfigurationException("Component " + componentName + " is already registered");
      }

      commitWrapperInstanceChange(wrapper, instance, metadata, WrapperState.INSTANTIATING, WrapperState.INSTANTIATED);

      wireWrapper(wrapper);
      return wrapper;
   }

   private ComponentMetadata getMetadataForComponent(Object component) {
      return component != null ? metadataRepo.getComponentMetadata(component.getClass()) : null;
   }

   @Override
   public void registerAlias(String aliasName, String targetComponentName, Class<?> targetComponentType) {
      ComponentWrapper wrapper = registerWrapper(aliasName, false);

      if (!prepareWrapperChange(wrapper, WrapperState.EMPTY, WrapperState.INSTANTIATING)) {
         throw new IllegalStateException("Cannot register alias " + aliasName + " with target " + targetComponentName +
                                         " as the name is already registered");
      }
      commitWrapperAliasChange(wrapper, ComponentAlias.of(targetComponentName), null, WrapperState.INSTANTIATING,
                               WrapperState.INSTANTIATED);
   }

   @Override
   public void addDynamicDependency(String ownerComponentName, String dependencyComponentName) {
      ComponentRef<Object> ref = getComponent0(ownerComponentName, Object.class, false);
      if (ref instanceof ComponentWrapper) {
         ComponentWrapper wrapper = (ComponentWrapper) ref;
         lock.lock();
         try {
            wrapper.addDynamicDependency(dependencyComponentName);
         } finally {
            lock.unlock();
         }
      }
   }

   @Override
   public void replaceComponent(String componentName, Object newInstance, boolean manageLifecycle) {
      boolean start;
      lock.lock();
      try {
         ComponentWrapper oldWrapper = components.remove(componentName);
         start = oldWrapper != null && oldWrapper.isRunning();
      } finally {
         lock.unlock();
      }

      // This is not thread-safe, but it's good enough for testing
      registerComponent(componentName, newInstance, manageLifecycle);
      if (start) {
         getComponent(componentName, newInstance.getClass()).running();
      }
   }

   @Override
   public void rewire() {
      // Rewire is not supposed to be used in production, so we can keep the registry locked for the entire duration
      lock.lock();
      try {
         if (status == ComponentStatus.TERMINATED) {
            status = ComponentStatus.RUNNING;
         }
         for (ComponentWrapper wrapper : components.values()) {
            performInjection(wrapper.instance, wrapper.metadata, false);
         }
      } finally {
         lock.unlock();
      }
   }

   @Override
   public Collection<ComponentRef<?>> getRegisteredComponents() {
      lock.lock();
      try {
         List<ComponentRef<?>> list = new ArrayList<>(components.size());
         for (ComponentWrapper wrapper : components.values()) {
            list.add(wrapper);
         }
         return list;
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void stop() {
      ArrayList<String> componentsToStop;
      lock.lock();
      try {
         if (status != ComponentStatus.RUNNING) {
            throw new IllegalStateException(
               "Stopping is only allowed in the RUNNING state, current state is " + status + "!");
         }

         componentsToStop = new ArrayList<>(startedComponents);
         status = ComponentStatus.STOPPING;
         condition.signalAll();
      } finally {
         lock.unlock();
      }

      for (int i = componentsToStop.size() - 1; i >= 0; i--) {
         ComponentWrapper wrapper = components.get(componentsToStop.get(i));
         stopWrapper(wrapper);
      }

      lock.lock();
      try {
         startedComponents.clear();
         removeVolatileComponents();

         status = ComponentStatus.TERMINATED;
         condition.signalAll();
      } finally {
         lock.unlock();
      }
   }

   @GuardedBy("lock")
   private void removeVolatileComponents() {
      for (Iterator<ComponentWrapper> it = components.values().iterator(); it.hasNext(); ) {
         ComponentWrapper wrapper = it.next();
         boolean survivesRestarts = wrapper.metadata != null && wrapper.metadata.isSurvivesRestarts();
         if (wrapper.manageLifecycle && !survivesRestarts) {
            if (trace)
               log.tracef("Removing component %s in state %s", wrapper.name, wrapper.state);
            it.remove();
         } else {
            if (wrapper.manageLifecycle && wrapper.state == WrapperState.STOPPED) {
               wrapper.state = WrapperState.INSTANTIATED;
            }
            if (trace)
               log.tracef("Keeping component %s in state %s", wrapper.name, wrapper.state);
         }
      }
   }

   void startWrapper(ComponentWrapper wrapper) {
      if (!prepareWrapperChange(wrapper, WrapperState.WIRED, WrapperState.STARTING)) {
         // Someone else is starting the wrapper, wait for it to finish
         awaitWrapperState(wrapper, WrapperState.STARTED);
         return;
      }

      if (wrapper.aliasTarget != null) {
         wrapper.aliasTarget.running();
         commitWrapperStateChange(wrapper, WrapperState.STARTING, WrapperState.STARTED);
         return;
      }

      if (wrapper.metadata == null) {
         commitWrapperStateChange(wrapper, WrapperState.STARTING, WrapperState.FAILED);
         throw new IllegalStateException("Components without metadata should go directly to RUNNING state");
      }

      startDependencies(wrapper);

      if (!wrapper.manageLifecycle) {
         commitWrapperStateChange(wrapper, WrapperState.STARTING, WrapperState.STARTED);
         return;
      }

      if (wrapper.metadata.getPostStartMethods().length > 0) {
         log.warnf("Running post-start methods on class %s as start methods, in the future the @PostStart " +
                   "annotation will be ignored", wrapper.metadata.getName());
      }

      try {
         for (ComponentMetadata.PrioritizedMethodMetadata method : wrapper.metadata.getStartMethods()) {
            ReflectionUtil.invokeAccessibly(wrapper.instance, method.getMethod(), null);
         }
         for (ComponentMetadata.PrioritizedMethodMetadata method : wrapper.metadata.getPostStartMethods()) {
            ReflectionUtil.invokeAccessibly(wrapper.instance, method.getMethod(), null);
         }

         logStartedComponent(wrapper);

         commitWrapperStateChange(wrapper, WrapperState.STARTING, WrapperState.STARTED);
      } catch (Throwable t) {
         commitWrapperStateChange(wrapper, WrapperState.STARTING, WrapperState.FAILED);
         throw t;
      }
   }

   private void logStartedComponent(ComponentWrapper wrapper) {
      lock.lock();
      try {
         startedComponents.add(wrapper.getName());
      } finally {
         lock.unlock();
      }
   }

   private void startDependencies(ComponentWrapper wrapper) {
      for (ComponentMetadata.InjectFieldMetadata injectField : wrapper.metadata.getInjectFields()) {
         String name = injectField.getDependencyName();
         if (!injectField.isLazy()) {
            ComponentRef<?> dependency = getComponent(name, injectField.getComponentClass());
            if (dependency != null) {
               dependency.running();
            }
         }
      }
      for (ComponentMetadata.InjectMethodMetadata injectMethod : wrapper.metadata.getInjectMethods()) {
         Class<?>[] componentTypes = injectMethod.getParameterClasses();
         for (int i = 0; i < componentTypes.length; i++) {
            String name = injectMethod.getDependencyName(i);
            if (!injectMethod.getParameterLazy(i)) {
               ComponentRef<?> dependency = getComponent(name, componentTypes[i]);
               if (dependency != null) {
                  dependency.running();
               }
            }
         }
      }
      ComponentPath remainingDependencies = wrapper.dynamicDependencies;
      while (remainingDependencies != null) {
         String componentName = remainingDependencies.name;
         ComponentRef<Object> dependency = getComponent(componentName, Object.class);
         if (dependency != null) {
            dependency.running();
         }
         remainingDependencies = remainingDependencies.next;
      }
   }

   private void stopWrapper(ComponentWrapper wrapper) {
      if (!prepareWrapperChange(wrapper, WrapperState.STARTED, WrapperState.STOPPING))
         return;

      performStop(wrapper);

      commitWrapperStateChange(wrapper, WrapperState.STOPPING, WrapperState.STOPPED);
   }

   private void performStop(ComponentWrapper wrapper) {
      if (!wrapper.manageLifecycle || wrapper.metadata == null)
         return;

      try {
         for (ComponentMetadata.PrioritizedMethodMetadata method : wrapper.metadata.getStopMethods()) {
            ReflectionUtil.invokeAccessibly(wrapper.instance, method.getMethod(), null);
         }
      } catch (Exception e) {
         log.error("Error stopping component " + wrapper.name, e);
      }
   }

   private boolean prepareWrapperChange(ComponentWrapper wrapper, WrapperState expectedState, WrapperState newState) {
      lock.lock();
      try {
         if (status != ComponentStatus.RUNNING && newState.isBefore(WrapperState.STOPPING)) {
            throw new IllegalLifecycleStateException(
               "Cannot wire or start components while the registry is not running");
         }
         if (wrapper.state != expectedState) {
            return false;
         }

         wrapper.state = newState;

         ComponentPath currentPath = mutatorThreads.get(Thread.currentThread());
         String name = wrapper.name;
         String className = wrapper.instance != null ? wrapper.instance.getClass().getName() : null;
         mutatorThreads.put(Thread.currentThread(), new ComponentPath(name, className, currentPath));
         if (trace)
            log.tracef("Changed status of " + name + " to " + wrapper.state);
         return true;
      } finally {
         lock.unlock();
      }
   }

   private void awaitWrapperState(ComponentWrapper wrapper, WrapperState expectedState) {
      lock.lock();
      try {
         String name = wrapper.name;
         if (wrapper.state == WrapperState.EMPTY) {
            throw new CacheConfigurationException(
               "Component " + name + " is missing a strong reference: waiting to become " + expectedState +
               " but it has not been instantiated yet");
         }
         if (wrapper.state.isBefore(expectedState)) {
            ComponentPath currentComponentPath = mutatorThreads.get(Thread.currentThread());
            if (currentComponentPath != null && currentComponentPath.contains(name)) {
               String className = wrapper.instance != null ? wrapper.instance.getClass().getName() : null;
               throw new CacheConfigurationException(
                  "Dependency cycle detected, please use ComponentRef<T> to break the cycle in path " +
                  new ComponentPath(name, className, getCurrentComponentPath()));
            }
         }
         while (status == ComponentStatus.RUNNING && wrapper.isBefore(expectedState)) {
            try {
               condition.await();
            } catch (InterruptedException e) {
               throw new IllegalLifecycleStateException(
                  "Interrupted while waiting for component " + name + " to start");
            }
         }
         wrapper.expectState(expectedState, WrapperState.STOPPING);
      } finally {
         lock.unlock();
      }
   }

   private ComponentPath getCurrentComponentPath() {
      lock.lock();
      try {
         return mutatorThreads.get(Thread.currentThread());
      } finally {
         lock.unlock();
      }
   }

   enum WrapperState {
      // Most components go through all the states.
      // Aliases and components without metadata (i.e. no dependencies and no start methods)
      // skip the WIRED and STARTING states.
      EMPTY, INSTANTIATING, INSTANTIATED, WIRING, WIRED, STARTING, STARTED, STOPPING, STOPPED, FAILED;

      boolean isAtLeast(WrapperState other) {
         return this.ordinal() >= other.ordinal();
      }

      boolean isBefore(WrapperState other) {
         return this.ordinal() < other.ordinal();
      }
   }

   static class ComponentWrapper implements ComponentRef {
      private final BasicComponentRegistryImpl registry;
      private final String name;
      private final boolean manageLifecycle;

      // These can be changed only from null to non-null, and can be safely read without locking
      // Always valid
      private volatile WrapperState state;
      private volatile ComponentPath dynamicDependencies;
      // Valid from INSTANTIATED state
      private volatile Object instance;
      private volatile ComponentMetadata metadata;
      private volatile ComponentRef<?> aliasTarget;

      ComponentWrapper(BasicComponentRegistryImpl registry, String name, boolean manageLifecycle) {
         this.registry = registry;
         this.name = name;
         this.manageLifecycle = manageLifecycle;

         this.state = WrapperState.EMPTY;
      }

      @Override
      public Object running() {
         if (!isRunning()) {
            wire();

            registry.startWrapper(this);

            expectState(WrapperState.STARTED, WrapperState.STOPPING);
         }
         return aliasTarget != null ? aliasTarget.running() : instance;
      }

      @Override
      public Object wired() {
         if (!isWired()) {
            wire();
         }

         return aliasTarget != null ? aliasTarget.wired() : instance;
      }

      public void wire() {
         if (!isAtLeast(WrapperState.INSTANTIATED)) {
            ComponentFactory factory = registry.findFactory(name);
            registry.instantiateWrapper(this, factory);
         }

         registry.wireWrapper(this);
      }

      @Override
      public boolean isRunning() {
         return state == WrapperState.STARTED;
      }

      @Override
      public boolean isWired() {
         return isAtLeast(WrapperState.WIRED) && isBefore(WrapperState.STOPPING);
      }

      @Override
      public String getName() {
         return name;
      }

      void expectState(WrapperState firstAllowedState, WrapperState firstDisallowedState) {
         WrapperState localState = this.state;
         if (localState.isBefore(firstAllowedState)) {
            throw new IllegalLifecycleStateException("Component " + name + " is not yet " + firstAllowedState);
         }
         if (localState.isAtLeast(firstDisallowedState)) {
            throw new IllegalLifecycleStateException("Component " + name + " is already " + localState);
         }
      }

      boolean isAtLeast(WrapperState expectedState) {
         return state.isAtLeast(expectedState);
      }

      boolean isBefore(WrapperState expectedState) {
         return state.isBefore(expectedState);
      }

      @GuardedBy("lock")
      void addDynamicDependency(String subComponentName) {
         dynamicDependencies = new ComponentPath(subComponentName, null, dynamicDependencies);
      }

      @Override
      public String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append("ComponentWrapper{")
           .append("name=")
           .append(name);
         if (aliasTarget != null) {
            sb.append(", aliasTarget=").append(aliasTarget);
         } else {
            sb.append(", instance=").append(instance);
         }
         sb.append(", status=").append(state);
         sb.append('}');
         return sb.toString();
      }
   }

   static class ComponentPath {
      final String name;
      final String className;
      final ComponentPath next;

      ComponentPath(String name, String className, ComponentPath next) {
         this.name = name;
         this.className = className;
         this.next = next;
      }

      public boolean contains(String name) {
         ComponentPath path = this;
         while (path != null) {
            if (path.name.equals(name)) {
               return true;
            }
            path = path.next;
         }
         return false;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         boolean firstIteration = true;
         ComponentPath path = this;
         while (path != null) {
            if (firstIteration) {
               firstIteration = false;
            } else {
               sb.append("\n  << ");
            }
            sb.append(path.name);
            if (className != null) {
               sb.append(" (a ").append(path.className).append(")");
            }
            path = path.next;
         }
         return sb.toString();
      }
   }

   private static class DefaultFactoryFactory implements ComponentFactory {
      private final Class<? extends ComponentFactory> factoryClass;

      DefaultFactoryFactory(Class<? extends ComponentFactory> factoryClass) {
         this.factoryClass = factoryClass;
      }

      @Override
      public Object construct(String componentName) {
         assert factoryClass.getName().equals(componentName);
         try {
            return factoryClass.newInstance();
         } catch (InstantiationException | IllegalAccessException e) {
            throw new CacheConfigurationException("Unable to instantiate factory " + factoryClass.getName(), e);
         }
      }
   }
}
