package org.infinispan.factories.impl;

import java.util.Collection;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.util.Experimental;
import org.infinispan.factories.annotations.Inject;

/**
 * Basic dependency injection container.
 *
 * <p>Components are identified by a component name {@code String}, which is usually the fully-qualified name
 * of a class or interface.
 * Components may have aliases, e.g. the cache is visible both as {@code org.infinispan.Cache} and as
 * {@code org.infinispan.AdvancedCache}. </p>
 *
 * <p>Components can either be registered explicitly  with {@link #registerComponent(String, Object, boolean)},
 * or constructed by a factory and registered implicitly when another component depends on them.</p>
 *
 * <p>During registration (either explicit or implicit), fields and and methods annotated with {@link Inject}
 * are injected with other components.
 * The name of the dependency is usually the fully-qualified name of the field or parameter type,
 * but it can be overridden with the {@link org.infinispan.factories.annotations.ComponentName} annotation.</p>
 *
 * <p>A factory is a component implementing {@link org.infinispan.factories.ComponentFactory} and annotated with
 * {@link org.infinispan.factories.annotations.DefaultFactoryFor}.
 * A factory must have the same scope as the components it creates.
 * A factory implementing {@link org.infinispan.factories.AutoInstantiableFactory} does not have to be registered
 * explicitly, the registry will be create and register it on demand.</p>
 *
 * <p> Each registry has a scope, and {@link org.infinispan.factories.scopes.Scopes#NAMED_CACHE}-scoped registries
 * delegate to a {@link org.infinispan.factories.scopes.Scopes#GLOBAL} registry.
 * {@link #registerComponent(String, Object, boolean)} will throw a
 * {@linkplain org.infinispan.commons.CacheConfigurationException} if the component has a
 * {@link org.infinispan.factories.scopes.Scope} annotation withe a different scope.
 * Implicitly created dependencies are automatically registered in the right scope based on their
 * {@link org.infinispan.factories.scopes.Scope} annotation.</p>
 *
 * <p>Dependency cycles are not allowed. Declaring the dependency field of type {@link ComponentRef},
 * breaks the cycle by allowing the registry to inject/start the dependency lazily.</p>
 *
 * <p>For all the components that implement {@link Lifecycle}, the registry calls {@linkplain Lifecycle#start()} during
 * startup, in the order they were registered.
 * If a component is registered when the registry is already running, {@linkplain Lifecycle#start()} is called during
 * registration.</p>
 *
 * <p>During shutdown, registration of new components is not allowed.
 * The registry stops all the components in the reverse order of their start, by invoking all the methods annotated
 * with {@link org.infinispan.factories.annotations.Stop}.
 * The </p>
 *
 * <p>Methods annotated with {@link org.infinispan.factories.annotations.Start} and
 * {@link org.infinispan.factories.annotations.PostStart} a</p>
 */
public interface BasicComponentRegistry {
   /**
    * Looks up a running component named {@code name} in the registry, or registers it if necessary.
    *
    * <p>If another thread is registering the component, wait for the other thread to finish.</p>
    *
    * <p>The component is wired (dependencies are injected) during registration.
    * Use {@link ComponentRef#running()} to start the component.</p>
    *
    * @param name The component name.
    * @param componentType The expected component type, not used to identify the component.
    */
   <T> ComponentRef<T> getComponent(String name, Class<T> componentType);

   /**
    * Looks up a running component named {@code name} in the registry, or registers it if necessary.
    *
    * @implSpec Equivalent to {@code getComponent(componentType.getName(), componentType)}.
    */
   default <T> ComponentRef<T> getComponent(Class<T> componentType) {
      return getComponent(componentType.getName(), componentType);
   }

   /**
    * Register a component named {@code componentName}.
    *
    * <p>If the component has dependencies, look them up using {@link #getComponent(String, Class)}
    * and inject them.</p>
    *
    * @param componentName The component name.
    * @param instance The component instance.
    * @param manageLifecycle {@code false} if the registry should ignore methods annotated with
    * {@linkplain org.infinispan.factories.annotations.Start} and {@linkplain org.infinispan.factories.annotations.Stop}
    *
    * @throws org.infinispan.IllegalLifecycleStateException If the registry is stopping/stopped
    * @throws org.infinispan.commons.CacheConfigurationException If a component/alias is already registered with that
    *         name, or if a dependency cannot be resolved
    */
   <T> ComponentRef<T> registerComponent(String componentName, T instance, boolean manageLifecycle);

   /**
    * Register a component named {@code componentType.getName()}.
    *
    * @implSpec Equivalent to {@code registerComponent(componentType.getName(), instance, manageLifecycle)}
    */
   default <T> ComponentRef<T> registerComponent(Class<?> componentType, T instance, boolean manageLifecycle) {
      return registerComponent(componentType.getName(), instance, manageLifecycle);
   }

   /**
    * Register an alias to another component.
    *
    * <p>Components that depend on the alias will behave as if they depended on the original component directly.</p>
    *
    * @throws org.infinispan.IllegalLifecycleStateException If the registry is stopping/stopped
    * @throws org.infinispan.commons.CacheConfigurationException If a component/alias is already registered with that
    *         name
    */
   void registerAlias(String aliasName, String targetComponentName, Class<?> targetComponentType);

   /**
    * Look up the dependencies of {@code target} as if it were a component, and inject them.
    *
    * <p>Behaves as if every dependency was resolved with {@code getComponent(String, Class)}.</p>
    *
    * @param target An instance of a class with {@link Inject} annotations.
    * @param startDependencies If {@code true}, start the dependencies before injecting them.
    *
    * @throws org.infinispan.IllegalLifecycleStateException If the registry is stopping/stopped
    * @throws org.infinispan.commons.CacheConfigurationException If a dependency cannot be resolved
    */
   void wireDependencies(Object target, boolean startDependencies);

   /**
    * Add a dynamic dependency between two components.
    *
    * @param ownerComponentName The dependent component's name.
    * @param dependencyComponentName The component depended on.
    *
    * <p>Note: doesn't have any effect if the owner component is already started.
    * The stop order is determined exclusively by the start order.</p>
    */
   void addDynamicDependency(String ownerComponentName, String dependencyComponentName);

   /**
    * Replace an existing component.
    *
    * For testing purposes only, NOT THREAD-SAFE.
    *
    * <p>Dependencies will be injected, and the start/stop methods will run if {@code manageLifecycle} is {@code true}.
    * The new component is stopped exactly when the replaced component would have been stopped,
    * IGNORING DEPENDENCY CHANGES.
    * Need to call {@link #rewire()} to inject the new component in all the components that depend on it.
    * If the component is global, need to call {@link #rewire()} on all the cache component registries as well.</p>
    *
    * @throws org.infinispan.IllegalLifecycleStateException If the registry is stopping/stopped
    */
   @Experimental
   void replaceComponent(String componentName, Object newInstance, boolean manageLifecycle);

   /**
    * Rewire all the injections after a component was replaced with {@link #replaceComponent(String, Object, boolean)}.
    *
    * For testing purposes only.
    */
   @Experimental
   void rewire();

   /**
    * Run {@code consumer} for each registered component in the current scope.
    */
   @Experimental
   Collection<ComponentRef<?>> getRegisteredComponents();

   void stop();
}
