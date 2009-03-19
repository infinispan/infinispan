package org.horizon.factories;

import org.horizon.AdvancedCache;
import org.horizon.CacheException;
import org.horizon.config.Configuration;
import org.horizon.config.ConfigurationException;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.scopes.ScopeDetector;
import org.horizon.factories.scopes.Scopes;
import org.horizon.lifecycle.ComponentStatus;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.notifications.cachemanagerlistener.CacheManagerNotifier;

import java.util.HashMap;
import java.util.Map;

/**
 * Named cache specific components
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class ComponentRegistry extends AbstractComponentRegistry {

   // cached component scopes
   static final Map<Class, Scopes> componentScopeLookup = new HashMap<Class, Scopes>();

   GlobalComponentRegistry globalComponents;
   String cacheName;
   Log log = LogFactory.getLog(ComponentRegistry.class);
   CacheManagerNotifier cacheManagerNotifier;

   @Inject
   public void setCacheManagerNotifier(CacheManagerNotifier cacheManagerNotifier) {
      this.cacheManagerNotifier = cacheManagerNotifier;
   }

   /**
    * Creates an instance of the component registry.  The configuration passed in is automatically registered.
    *
    * @param configuration    configuration with which this is created
    * @param cache            cache
    * @param globalComponents Shared Component Registry to delegate to
    */
   public ComponentRegistry(String cacheName, Configuration configuration, AdvancedCache cache, GlobalComponentRegistry globalComponents) {

      try {
         this.cacheName = cacheName;
         if (cacheName == null) throw new ConfigurationException("Cache name cannot be null!");
         if (globalComponents == null) throw new NullPointerException("GlobalComponentRegistry cannot be null!");
         this.globalComponents = globalComponents;

         registerDefaultClassLoader(null);
         registerComponent(this, ComponentRegistry.class);
         registerComponent(configuration, Configuration.class);
         registerComponent(new BootstrapFactory(cache, configuration, this), BootstrapFactory.class);
      }
      catch (Exception e) {
         throw new CacheException("Unable to construct a ComponentRegistry!", e);
      }
   }

   protected Log getLog() {
      return log;
   }

   @Override
   public final <T> T getComponent(Class<T> componentType, String name) {
      if (isGlobal(componentType)) {
         return globalComponents.getComponent(componentType, name);
      } else {
         return getLocalComponent(componentType, name);
      }
   }

   @SuppressWarnings("unchecked")
   public final <T> T getLocalComponent(Class<T> componentType, String name) {
      Component wrapper = lookupLocalComponent(componentType, name);
      if (wrapper == null) return null;

      return (T) (wrapper.instance == NULL_COMPONENT ? null : wrapper.instance);
   }

   public final <T> T getLocalComponent(Class<T> componentType) {
      return getLocalComponent(componentType, componentType.getName());
   }

   @Override
   protected final Map<Class, Class<? extends AbstractComponentFactory>> getDefaultFactoryMap() {
      // delegate to parent.  No sense maintaining multiple copies of this map.
      return globalComponents.getDefaultFactoryMap();
   }

   @Override
   protected final Component lookupComponent(Class componentClass, String name) {
      if (isGlobal(componentClass)) {
         return globalComponents.lookupComponent(componentClass, name);
      } else {
         return lookupLocalComponent(componentClass, name);
      }
   }

   protected final Component lookupLocalComponent(Class componentClass, String name) {
      return super.lookupComponent(componentClass, name);
   }

   public final GlobalComponentRegistry getGlobalComponentRegistry() {
      return globalComponents;
   }

   @Override
   public final void registerComponent(Object component, String name) {
      if (isGlobal(component.getClass())) {
         globalComponents.registerComponent(component, name);
      } else {
         super.registerComponent(component, name);
      }
   }

   private boolean isGlobal(Class clazz) {
      Scopes componentScope = componentScopeLookup.get(clazz);
      if (componentScope == null) {
         componentScope = ScopeDetector.detectScope(clazz);
         componentScopeLookup.put(clazz, componentScope);
      }

      return componentScope == Scopes.GLOBAL;
   }

   @Override
   public void start() {
      if (globalComponents.getStatus() != ComponentStatus.RUNNING || globalComponents.getStatus() != ComponentStatus.INITIALIZING) {
         globalComponents.start();
      }
      boolean needToNotify = state != ComponentStatus.RUNNING && state != ComponentStatus.INITIALIZING;

      // set this up *before* starting the components since some components - specifically state transfer - needs to be
      // able to locate this registry via the InboundInvocationHandler
      globalComponents.registerNamedComponentRegistry(this, cacheName);

      super.start();
      if (needToNotify && state == ComponentStatus.RUNNING) {
         cacheManagerNotifier.notifyCacheStarted(cacheName);
      }
   }

   @Override
   public void stop() {
      if (state.stopAllowed()) globalComponents.unregisterNamedComponentRegistry(cacheName);
      boolean needToNotify = state == ComponentStatus.RUNNING || state == ComponentStatus.INITIALIZING;
      super.stop();
      if (state == ComponentStatus.TERMINATED && needToNotify) cacheManagerNotifier.notifyCacheStopped(cacheName);
   }
}
