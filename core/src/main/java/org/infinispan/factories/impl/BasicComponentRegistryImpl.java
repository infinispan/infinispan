package org.infinispan.factories.impl;

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

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.ModuleRepository;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * @author Dan Berindei
 * @since 8.2
 */
public class BasicComponentRegistryImpl implements BasicComponentRegistry {
   private static final Log log = LogFactory.getLog(BasicComponentRegistryImpl.class);

   private final ModuleRepository moduleRepository;
   private final Scopes scope;
   private final BasicComponentRegistry next;

   private final Lock lock = new ReentrantLock();
   private final Condition condition = lock.newCondition();
   // The map and can be used without the lock, but changes to the wrapper state need the lock
   private final ConcurrentMap<String, ComponentWrapper> components = new ConcurrentHashMap<>();
   @GuardedBy("lock")
   private final List<String> startedComponents = new ArrayList<>();
   private final ConcurrentMap<Thread, ComponentPath> mutatorThreads;
   // Needs to lock for updates but not for reads
   private volatile ComponentStatus status;

   private final WireContext lookup = new WireContext(this);

   public BasicComponentRegistryImpl(ModuleRepository moduleRepository,
                                     boolean isGlobal, BasicComponentRegistry next) {
      this.moduleRepository = moduleRepository;
      this.scope = isGlobal ? Scopes.GLOBAL : Scopes.NAMED_CACHE;
      this.next = next;
      this.status = ComponentStatus.RUNNING;
      this.mutatorThreads = new ConcurrentHashMap<>();

      // No way to look up the next scope's BasicComponentRegistry, but that's not a problem
      registerComponent(BasicComponentRegistry.class, this, false);
   }

   @Override
   public <T, U extends T> ComponentRef<T> getComponent(String name, Class<U> componentType) {
      return getComponent0(name, componentType, true);
   }

   @Override
   public <T> ComponentRef<T> lazyGetComponent(Class<T> componentType) {
      return getComponent0(componentType.getName(), componentType, false);
   }

   @Override
   public MBeanMetadata getMBeanMetadata(String className) {
      MBeanMetadata metadata = moduleRepository.getMBeanMetadata(className);
      if (metadata == null) {
         return null;
      }

      // collect attributes and operations from supers (in reverse order to ensure proper overriding)
      Map<String, MBeanMetadata.AttributeMetadata> attributes = new HashMap<>();
      Map<String, MBeanMetadata.OperationMetadata> operations = new HashMap<>();

      MBeanMetadata currentMetadata = metadata;
      for (;;) {
         for (MBeanMetadata.AttributeMetadata attribute : currentMetadata.getAttributes()) {
            MBeanMetadata.AttributeMetadata existingAttr = attributes.put(attribute.getName(), attribute);
            if (existingAttr != null) {
               throw new IllegalStateException("Overriding/duplicate JMX attributes are not allowed. Attribute "
                     + attribute.getName() + " already exists in a subclass of " + className);
            }
         }
         for (MBeanMetadata.OperationMetadata operation : currentMetadata.getOperations()) {
            MBeanMetadata.OperationMetadata existingOp = operations.put(operation.getSignature(), operation);
            if (existingOp != null) {
               throw new IllegalStateException("Overriding/duplicate JMX operations are not allowed. Operation "
                     + operation.getSignature() + " already exists in a subclass of " + className);
            }
         }
         className = currentMetadata.getSuperMBeanClassName();
         if (className == null) {
            break;
         }
         currentMetadata = moduleRepository.getMBeanMetadata(className);
         if (currentMetadata == null) {
            throw new IllegalStateException("Missing MBean metadata for class " + className);
         }
      }

      return new MBeanMetadata(metadata.getJmxObjectName(), metadata.getDescription(), null,
            attributes.values(), operations.values());
   }

   <T, U extends T> ComponentRef<T> getComponent0(String name, Class<U> componentType, boolean needInstance) {
      ComponentWrapper wrapper = components.get(name);
      if (wrapper != null && (wrapper.isAtLeast(WrapperState.WIRED) || !needInstance)) {
         // The wrapper already exists, return it even if the instance was not wired or even created yet
         return wrapper;
      }

      if (wrapper == null) {
         // The wrapper doesn't yet exist in the current scope
         // Try to find/construct the component in the next scope
         if (next != null) {
            ComponentRef<T> nextScopeRef = next.getComponent(name, componentType);
            if (nextScopeRef != null) {
               return nextScopeRef;
            }
         }
      }

      ComponentFactory factory = findFactory(name);
      if (wrapper == null) {
         if (factory == null) {
            // Return null without registering a wrapper so the previous scope can have a go
            // It's ok to return null if another thread is registering the component concurrently
            return null;
         }

         // Create the wrapper in the current scope
         wrapper = registerWrapper(name, true);
      }

      if (needInstance) {
         if (factory != null) {
            instantiateWrapper(wrapper, factory);
         } else {
            // If the wrapper exists but the factory is null, another thread must be registering the component,
            // either manually or from tryAutoInstantiation
            awaitWrapperState(wrapper, WrapperState.INSTANTIATED);
         }
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

   private void instantiateWrapper(ComponentWrapper wrapper, ComponentFactory factory) {
      String name = wrapper.name;
      if (!prepareWrapperChange(wrapper, WrapperState.EMPTY, WrapperState.INSTANTIATING)) {
         // Someone else has started instantiating and wiring the component, wait for them to finish
         awaitWrapperState(wrapper, WrapperState.INSTANTIATED);
         return;
      }

      try {
         // Also changes the status when successful
         doInstantiateWrapper(wrapper, factory, name);
      } catch (Throwable t) {
         commitWrapperStateChange(wrapper, WrapperState.INSTANTIATING, WrapperState.FAILED);
         throw t;
      }
   }

   private void doInstantiateWrapper(ComponentWrapper wrapper, ComponentFactory factory, String name) {
      Object instance;
      try {
         instance = factory.construct(name);
      } catch (Throwable t) {
         throw new CacheConfigurationException(
               "Failed to construct component " + name + ", path " + peekMutatorPath(), t);
      }

      if (instance instanceof ComponentAlias) {
         // Create the target component and point this wrapper to it
         ComponentAlias alias = (ComponentAlias) instance;
         commitWrapperAliasChange(wrapper, alias, null, WrapperState.INSTANTIATING, WrapperState.INSTANTIATED);
      } else {
         ComponentAccessor<Object> accessor = getMetadataForComponent(instance);
         commitWrapperInstanceChange(wrapper, instance, accessor, WrapperState.INSTANTIATING,
                                     WrapperState.INSTANTIATED);
      }
   }

   private void wireWrapper(ComponentWrapper wrapper) {
      if (!prepareWrapperChange(wrapper, WrapperState.INSTANTIATED, WrapperState.WIRING)) {
         // Someone else has started wiring the component, wait for them to finish
         awaitWrapperState(wrapper, WrapperState.WIRED);
         return;
      }

      try {
         // Also changes the status when successful
         doWireWrapper(wrapper);
      } catch (Throwable t) {
         commitWrapperStateChange(wrapper, WrapperState.WIRING, WrapperState.FAILED);
         throw t;
      }
   }

   private void doWireWrapper(ComponentWrapper wrapper) {
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
         invokeInjection(wrapper.instance, wrapper.accessor, false);

         WrapperState wiredState = wrapper.accessor != null ? WrapperState.WIRED : WrapperState.STARTED;
         commitWrapperStateChange(wrapper, WrapperState.WIRING, wiredState);
      }
   }

   @Override
   public void wireDependencies(Object target, boolean startDependencies) {
      String componentClassName = target.getClass().getName();
      pushMutatorPath("wireDependencies", componentClassName);
      try {
         ComponentAccessor<Object> accessor = moduleRepository.getComponentAccessor(componentClassName);
         invokeInjection(target, accessor, startDependencies);
      } finally {
         popMutatorPath();
      }
   }

   private ComponentFactory findFactory(String name) {
      String factoryName = moduleRepository.getFactoryName(name);
      if (factoryName == null) {
         // No designated factory, try auto instantiation as a last resort
         return tryAutoInstantiation(name);
      }

      ComponentRef<ComponentFactory> factoryRef = getComponent(factoryName, ComponentFactory.class);
      if (factoryRef != null) {
         // Start the factory (and it's dependencies!) because some factories
         // are responsible for stopping the components they create (e.g. NamedExecutorsFactory)
         return factoryRef.running();
      } else {
         // The factory does not exist in this scope
         return null;
      }
   }

   private void commitWrapperStateChange(ComponentWrapper wrapper, WrapperState expectedState, WrapperState newState) {
      commitWrapperChange(wrapper, wrapper.instance, wrapper.accessor, wrapper.aliasTarget, expectedState, newState);
   }

   private void commitWrapperInstanceChange(ComponentWrapper wrapper, Object instance,
                                            ComponentAccessor<Object> accessor,
                                            WrapperState expectedState, WrapperState newState) {
      commitWrapperChange(wrapper, instance, accessor, null, expectedState, newState);
   }

   private void commitWrapperAliasChange(ComponentWrapper wrapper, ComponentAlias alias, ComponentRef<?> targetRef,
                                         WrapperState expectedState, WrapperState newState) {
      commitWrapperChange(wrapper, alias, null, targetRef, expectedState, newState);
   }

   private void commitWrapperChange(ComponentWrapper wrapper, Object instance, ComponentAccessor<Object> accessor,
                                    ComponentRef<?> aliasTargetRef,
                                    WrapperState expectedState, WrapperState newState) {
      lock.lock();
      try {
         if (wrapper.state != expectedState) {
            throw new IllegalLifecycleStateException(
               "Component " + wrapper.name + " has wrong status: " + wrapper.state + ", expected: " + expectedState);
         }
         wrapper.instance = instance;
         wrapper.accessor = accessor;
         wrapper.aliasTarget = aliasTargetRef;
         wrapper.state = newState;
         popMutatorPath();
         if (log.isTraceEnabled())
            log.tracef("Changed status of " + wrapper.name + " to " + wrapper.state);
         condition.signalAll();
      } finally {
         lock.unlock();
      }
   }

   /**
    * Factories that implement {@link org.infinispan.factories.AutoInstantiableFactory} can be instantiated
    * without an additional factory.
    */
   private ComponentFactory tryAutoInstantiation(String factoryName) {
      ComponentAccessor<Object> accessor = moduleRepository.getComponentAccessor(factoryName);
      if (accessor == null) {
         return null;
      }
      if (accessor.getScopeOrdinal() != null && !accessor.getScopeOrdinal().equals(scope.ordinal())) {
         // The accessor exists, but it has a different scope (null means any scope)
         // Returning null allows the factory to be auto-instantiated in the proper scope
         return null;
      }

      Object autoInstance = accessor.newInstance();
      if (autoInstance == null) {
         // Not auto-instantiable
         return null;
      }

      return new ConstComponentFactory(autoInstance);
   }

   private void invokeInjection(Object target, ComponentAccessor<Object> accessor, boolean startDependencies) {
      try {
         if (accessor == null)
            return;

         accessor.wire(target, lookup, startDependencies);
         String superComponentClassName = accessor.getSuperAccessorName();
         if (superComponentClassName != null) {
            ComponentAccessor<Object> superAccessor = moduleRepository.getComponentAccessor(superComponentClassName);
            if (superAccessor == null) {
               throw new CacheConfigurationException("Component metadata not found for super class " +
                                                     superComponentClassName);
            }
            invokeInjection(target, superAccessor, startDependencies);
         }
      } catch (IllegalLifecycleStateException | CacheConfigurationException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheConfigurationException(
            "Unable to inject dependencies for component class " + target.getClass().getName() + ", path " +
            peekMutatorPath(), e);
      }
   }

   <T> T throwDependencyNotFound(String componentName) {
      throw new CacheConfigurationException(
         "Unable to construct dependency " + componentName + " in scope " + scope + " for " +
         peekMutatorPath());
   }

   @Override
   public <T> ComponentRef<T> registerComponent(String componentName, T instance, boolean manageLifecycle) {
      ComponentAccessor<Object> accessor = getMetadataForComponent(instance);

      // Try to register the wrapper, but it may have been created as a lazy dependency of another component already
      ComponentWrapper wrapper = registerWrapper(componentName, manageLifecycle);
      if (!prepareWrapperChange(wrapper, WrapperState.EMPTY, WrapperState.INSTANTIATING)) {
         throw new CacheConfigurationException("Component " + componentName + " is already registered");
      }

      commitWrapperInstanceChange(wrapper, instance, accessor, WrapperState.INSTANTIATING, WrapperState.INSTANTIATED);

      wireWrapper(wrapper);
      return wrapper;
   }

   private ComponentAccessor<Object> validateAccessor(ComponentAccessor<Object> accessor, Class<?> componentClass) {
      // A missing scope declaration is interpreted as any scope
      String className = componentClass.getName();
      if (accessor != null && !accessor.getScopeOrdinal().equals(Scopes.NONE.ordinal()) &&
          !accessor.getScopeOrdinal().equals(scope.ordinal())) {
         throw new CacheConfigurationException(
            "Wrong registration scope " + scope + " for component class " + className);
      }

      // Special case for classes generated by Mockito
      if (accessor == null && className.contains("$MockitoMock$")) {
         Class<?> mockedClass = componentClass.getSuperclass();
         return validateAccessor(moduleRepository.getComponentAccessor(mockedClass.getName()), mockedClass);
      }

      // TODO It would be nice to log an exception if the component is annotated yet doesn't have an accessor
      // but that would require a runtime dependency on the component-annotations module.
      return accessor;
   }

   private ComponentAccessor<Object> getMetadataForComponent(Object component) {
      if (component == null)
         return null;

      Class<?> componentClass = component.getClass();
      ComponentAccessor<Object> accessor = moduleRepository.getComponentAccessor(componentClass.getName());

      return validateAccessor(accessor, componentClass);
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
      lock.lock();
      ComponentWrapper newWrapper = null;
      try {
         ComponentAccessor<Object> accessor = getMetadataForComponent(newInstance);
         invokeInjection(newInstance, accessor, false);

         newWrapper = new ComponentWrapper(this, componentName, manageLifecycle);
         ComponentWrapper oldWrapper = components.put(componentName, newWrapper);
         prepareWrapperChange(newWrapper, WrapperState.EMPTY, WrapperState.INSTANTIATING);

         // If the component was already started, start the replacement as well
         // but avoid logStartedComponent in order to preserve the stop order
         boolean wantStarted = oldWrapper != null && oldWrapper.isRunning();
         boolean canRunStart = manageLifecycle && accessor != null;
         if (wantStarted && canRunStart) {
            invokeStart(newInstance, accessor);
         }

         WrapperState state = (wantStarted || !canRunStart) ? WrapperState.STARTED : WrapperState.WIRED;
         commitWrapperInstanceChange(newWrapper, newInstance, accessor, WrapperState.INSTANTIATING, state);
      } catch (Throwable t) {
         if (newWrapper != null) {
            commitWrapperStateChange(newWrapper, WrapperState.INSTANTIATING, WrapperState.FAILED);
         }
         throw new CacheConfigurationException("Unable to start replacement component " + newInstance, t);
      } finally {
         lock.unlock();
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
            if (wrapper.isAlias()) {
               // Duplicates the code in doWireWrapper(), but without the state change
               ComponentAlias alias = (ComponentAlias) wrapper.instance;
               String aliasTargetName = alias.getComponentName();
               ComponentRef<Object> targetRef = getComponent(aliasTargetName, Object.class);
               if (targetRef == null) {
                  throw new CacheConfigurationException(
                        "Alias " + wrapper.name + " target component is missing: " + aliasTargetName);
               }
               targetRef.wired();
               wrapper.aliasTarget = targetRef;
            } else {
               invokeInjection(wrapper.instance, wrapper.accessor, false);
            }
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
         if (wrapper != null) {
            stopWrapper(wrapper);
         }
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

   @Override
   public boolean hasComponentAccessor(String componentClassName) {
      return moduleRepository.getComponentAccessor(componentClassName) != null;
   }

   @GuardedBy("lock")
   private void removeVolatileComponents() {
      for (Iterator<ComponentWrapper> it = components.values().iterator(); it.hasNext(); ) {
         ComponentWrapper wrapper = it.next();
         boolean survivesRestarts = wrapper.accessor != null && wrapper.accessor.getSurvivesRestarts();
         if (wrapper.manageLifecycle && !survivesRestarts) {
            if (log.isTraceEnabled())
               log.tracef("Removing component %s in state %s", wrapper.name, wrapper.state);
            it.remove();
         } else {
            if (wrapper.manageLifecycle && wrapper.state == WrapperState.STOPPED) {
               wrapper.state = WrapperState.INSTANTIATED;
            }
            if (log.isTraceEnabled())
               log.tracef("Keeping component %s in state %s", wrapper.name, wrapper.state);
         }
      }
   }

   private void startWrapper(ComponentWrapper wrapper) {
      if (!prepareWrapperChange(wrapper, WrapperState.WIRED, WrapperState.STARTING)) {
         // Someone else is starting the wrapper, wait for it to finish
         awaitWrapperState(wrapper, WrapperState.STARTED);
         return;
      }

      try {
         doStartWrapper(wrapper);

         commitWrapperStateChange(wrapper, WrapperState.STARTING, WrapperState.STARTED);
      } catch (CacheException e) {
         commitWrapperStateChange(wrapper, WrapperState.STARTING, WrapperState.FAILED);
         throw e;
      } catch (Throwable t) {
         commitWrapperStateChange(wrapper, WrapperState.STARTING, WrapperState.FAILED);
         throw new CacheConfigurationException("Error starting component " + wrapper.name, t);
      }
   }

   private void doStartWrapper(ComponentWrapper wrapper) throws Exception {
      if (wrapper.aliasTarget != null) {
         wrapper.aliasTarget.running();
         return;
      }

      if (wrapper.accessor == null) {
         throw new IllegalStateException("Components without metadata should go directly to RUNNING state");
      }

      startDependencies(wrapper);

      if (!wrapper.manageLifecycle) {
         return;
      }

      // Try to stop the component even if it failed, otherwise each component would have to catch exceptions
      logStartedComponent(wrapper);

      invokeStart(wrapper.instance, wrapper.accessor);
   }

   private void invokeStart(Object instance, ComponentAccessor<Object> accessor) throws Exception {
      // Invoke super first
      if (accessor.getSuperAccessorName() != null) {
         invokeStart(instance, moduleRepository.getComponentAccessor(accessor.getSuperAccessorName()));
      }

      accessor.start(instance);
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
      ComponentAccessor<Object> accessor = wrapper.accessor;
      while (true) {
         for (String dependencyName : accessor.getEagerDependencies()) {
            ComponentRef<Object> dependency = getComponent(dependencyName, Object.class);
            if (dependency != null) {
               dependency.running();
            }
         }

         if (accessor.getSuperAccessorName() == null)
            break;

         accessor = moduleRepository.getComponentAccessor(accessor.getSuperAccessorName());
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
      if (!prepareWrapperChange(wrapper, WrapperState.STARTED, WrapperState.STOPPING)
          && !prepareWrapperChange(wrapper, WrapperState.FAILED, WrapperState.STOPPING))
         return;

      try {
         doStopWrapper(wrapper);
      } catch (Throwable t) {
         log.errorf(t, "Error stopping component %s", wrapper.name);
      } finally {
         commitWrapperStateChange(wrapper, WrapperState.STOPPING, WrapperState.STOPPED);
      }
   }

   private void doStopWrapper(ComponentWrapper wrapper) throws Exception {
      if (!wrapper.manageLifecycle || wrapper.accessor == null)
         return;

      invokeStop(wrapper.instance, wrapper.accessor);
   }

   private void invokeStop(Object instance, ComponentAccessor<Object> accessor) throws Exception {
      accessor.stop(instance);

      // Invoke super last
      String superComponentClassName = accessor.getSuperAccessorName();
      if (superComponentClassName != null) {
         invokeStop(instance, moduleRepository.getComponentAccessor(superComponentClassName));
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

         String componentClassName = wrapper.instance != null ? wrapper.instance.getClass().getName() : null;
         String name = pushMutatorPath(wrapper.name, componentClassName);
         if (log.isTraceEnabled())
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
                  "Component " + name + " is missing a strong (non-ComponentRef) reference: waiting to become " +
                  expectedState + " but it has not been instantiated yet");
         }
         if (wrapper.state.isBefore(expectedState)) {
            ComponentPath currentComponentPath = peekMutatorPath();
            if (currentComponentPath != null && currentComponentPath.contains(name)) {
               String className = wrapper.instance != null ? wrapper.instance.getClass().getName() : null;
               throw new CacheConfigurationException(
                  "Dependency cycle detected, please use ComponentRef<T> to break the cycle in path " +
                  new ComponentPath(name, className, peekMutatorPath()));
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

   private String pushMutatorPath(String name, String className) {
      ComponentPath currentPath = mutatorThreads.get(Thread.currentThread());
      mutatorThreads.put(Thread.currentThread(), new ComponentPath(name, className, currentPath));
      return name;
   }

   private void popMutatorPath() {
      ComponentPath currentPath = mutatorThreads.get(Thread.currentThread());
      if (currentPath.next != null) {
         mutatorThreads.put(Thread.currentThread(), currentPath.next);
      } else {
         mutatorThreads.remove(Thread.currentThread());
      }
   }

   private ComponentPath peekMutatorPath() {
      return mutatorThreads.get(Thread.currentThread());
   }

   @Override
   public String toString() {
      return "BasicComponentRegistryImpl{scope=" + scope + ", size=" + components.size() + "}";
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
      private volatile ComponentAccessor<Object> accessor;
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
      public boolean isAlias() {
         return instance instanceof ComponentAlias;
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

      @GuardedBy("BasicComponentRegistryImpl.lock")
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
            if (path.className != null) {
               sb.append(" (a ").append(path.className).append(")");
            }
            path = path.next;
         }
         return sb.toString();
      }
   }

   private static class ConstComponentFactory implements ComponentFactory {
      private final Object autoInstance;

      public ConstComponentFactory(Object autoInstance) {
         this.autoInstance = autoInstance;
      }

      @Override
      public Object construct(String componentName) {
         return autoInstance;
      }
   }
}
