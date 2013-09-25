package org.infinispan.factories;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.components.ComponentMetadata;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.ref.WeakReference;
import java.util.Map;

import static org.infinispan.factories.KnownComponentNames.MODULE_COMMAND_INITIALIZERS;

/**
 * Named cache specific components
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ComponentRegistry extends AbstractComponentRegistry {

   private final GlobalComponentRegistry globalComponents;
   private final String cacheName;
   private static final Log log = LogFactory.getLog(ComponentRegistry.class);
   private CacheManagerNotifier cacheManagerNotifier;

   //Cached fields:
   private StreamingMarshaller cacheMarshaler;
   private StateTransferManager stateTransferManager;
   private ResponseGenerator responseGenerator;
   private CommandsFactory commandsFactory;
   private TotalOrderManager totalOrderManager;
   private StateTransferLock stateTransferLock;

   protected final WeakReference<ClassLoader> defaultClassLoader;

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
   public ComponentRegistry(String cacheName, Configuration configuration, AdvancedCache<?, ?> cache,
                            GlobalComponentRegistry globalComponents, ClassLoader defaultClassLoader) {
      this.defaultClassLoader = new WeakReference<ClassLoader>(defaultClassLoader);
      try {
         this.cacheName = cacheName;
         if (cacheName == null) throw new CacheConfigurationException("Cache name cannot be null!");
         if (globalComponents == null) throw new NullPointerException("GlobalComponentRegistry cannot be null!");
         this.globalComponents = globalComponents;

         registerComponent(this, ComponentRegistry.class);
         registerComponent(configuration, Configuration.class);
         registerComponent(new BootstrapFactory(cache, configuration, this), BootstrapFactory.class);

         // register any module-specific command initializers
         // Modules are on the same classloader as Infinispan
         Map<Byte, ModuleCommandInitializer> initializers = globalComponents.getModuleCommandInitializers();
         if (initializers != null && !initializers.isEmpty()) {
            registerNonVolatileComponent(initializers, MODULE_COMMAND_INITIALIZERS);
            for (ModuleCommandInitializer mci: initializers.values()) registerNonVolatileComponent(mci, mci.getClass());
         } else
            registerNonVolatileComponent(
                  InfinispanCollections.emptyMap(), MODULE_COMMAND_INITIALIZERS);
      }
      catch (Exception e) {
         throw new CacheException("Unable to construct a ComponentRegistry!", e);
      }
   }

   @Override
   protected ClassLoader getClassLoader() {
      return defaultClassLoader.get();
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   @SuppressWarnings("unchecked")
   public final <T> T getComponent(String componentTypeName, String name, boolean nameIsFQCN) {
      if (isGlobal(componentTypeName, name, nameIsFQCN)) {
         return (T) globalComponents.getComponent(componentTypeName, name, nameIsFQCN);
      } else {
         return (T) getLocalComponent(componentTypeName, name, nameIsFQCN);
      }
   }

   @SuppressWarnings("unchecked")
   public final <T> T getLocalComponent(String componentTypeName, String name, boolean nameIsFQCN) {
      return (T) super.getComponent(componentTypeName, name, nameIsFQCN);
   }

   @SuppressWarnings("unchecked")
   public final <T> T getLocalComponent(Class<T> componentType) {
      String componentTypeName = componentType.getName();
      return (T) getLocalComponent(componentTypeName, componentTypeName, true);
   }

   @Override
   protected final Component lookupComponent(String componentClassName, String name, boolean nameIsFQCN) {
      if (isGlobal(componentClassName, name, nameIsFQCN)) {
         log.tracef("Looking up global component %s", componentClassName);
         return globalComponents.lookupComponent(componentClassName, name, nameIsFQCN);
      } else {
         log.tracef("Looking up local component %s", componentClassName);
         return lookupLocalComponent(componentClassName, name, nameIsFQCN);
      }
   }

   protected final Component lookupLocalComponent(String componentClassName, String name, boolean nameIsFQCN) {
      return super.lookupComponent(componentClassName, name, nameIsFQCN);
   }

   public final GlobalComponentRegistry getGlobalComponentRegistry() {
      return globalComponents;
   }

   @Override
   protected final <T> T getOrCreateComponent(Class<T> componentClass, String name, boolean nameIsFQCN) {
      if (isGlobal(componentClass.getName(), name, nameIsFQCN)) {
         log.tracef("Get or create global component %s", componentClass);
         return globalComponents.getOrCreateComponent(componentClass, name, nameIsFQCN);
      } else {
         log.tracef("Get or create local component %s", componentClass);
         return super.getOrCreateComponent(componentClass, name, nameIsFQCN);
      }
   }

   private <T> boolean isGlobal(String componentClassName, String name, boolean nameIsFQCN) {
      if (!nameIsFQCN) {
         for (String s : KnownComponentNames.PER_CACHE_COMPONENT_NAMES) {
            if (s.equals(name))
               return false;
         }
      }
      return isGlobal(nameIsFQCN ? name : componentClassName);
   }

   @Override
   protected AbstractComponentFactory getFactory(Class<?> componentClass) {
      String cfClass = getComponentMetadataRepo().findFactoryForComponent(componentClass);
      if (cfClass == null) {
         throwStackAwareConfigurationException("No registered default factory for component '" + componentClass + "' found!");
      }

      AbstractComponentFactory cf;
      if (isGlobal(cfClass)) {
         log.tracef("Looking up global factory for component %s", componentClass);
         cf = globalComponents.getFactory(componentClass);
      } else {
         log.tracef("Looking up local factory for component %s", componentClass);
         cf = super.getFactory(componentClass);
      }
      return cf;
   }

   @Override
   protected final void registerComponentInternal(Object component, String name, boolean nameIsFQCN) {
      if (isGlobal(component.getClass().getName(), name, nameIsFQCN)) {
         globalComponents.registerComponentInternal(component, name, nameIsFQCN);
      } else {
         super.registerComponentInternal(component, name, nameIsFQCN);
      }
   }

   @Override
   protected AbstractComponentFactory createComponentFactoryInternal(Class<?> componentClass, String cfClass) {
      if (isGlobal(cfClass)) {
         return globalComponents.createComponentFactoryInternal(componentClass, cfClass);
      }
      return super.createComponentFactoryInternal(componentClass, cfClass);
   }

   private boolean isGlobal(String className) {
      ComponentMetadata m = getComponentMetadataRepo().findComponentMetadata(className);
      return m != null && m.isGlobalScope();
   }

   @Override
   public void start() {
      globalComponents.start();
      boolean needToNotify = state != ComponentStatus.RUNNING && state != ComponentStatus.INITIALIZING;

      // set this up *before* starting the components since some components - specifically state transfer - needs to be
      // able to locate this registry via the InboundInvocationHandler
      cacheComponents();
      this.globalComponents.registerNamedComponentRegistry(this, cacheName);

      notifyCacheStarting(getComponent(Configuration.class));

      super.start();

      if (needToNotify && state == ComponentStatus.RUNNING) {
         for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
            l.cacheStarted(this, cacheName);
         }
         cacheManagerNotifier.notifyCacheStarted(cacheName);
      }
   }

   void notifyCacheStarting(Configuration configuration) {
      for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
         l.cacheStarting(this, configuration, cacheName);
      }
   }

   @Override
   public void stop() {
      if (state.stopAllowed()) globalComponents.unregisterNamedComponentRegistry(cacheName);
      boolean needToNotify = state == ComponentStatus.RUNNING || state == ComponentStatus.INITIALIZING;
      if (needToNotify) {
         for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
            l.cacheStopping(this, cacheName);
         }
      }
      super.stop();
      if (state == ComponentStatus.TERMINATED && needToNotify) {
         for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
            l.cacheStopped(this, cacheName);
         }
         cacheManagerNotifier.notifyCacheStopped(cacheName);
      }
   }

   @Override
   public TimeService getTimeService() {
      return globalComponents.getTimeService();
   }

   public String getCacheName() {
      return cacheName;
   }

   /**
    * Caching shortcut for #getComponent(StreamingMarshaller.class, KnownComponentNames.CACHE_MARSHALLER);
    */
   public StreamingMarshaller getCacheMarshaller() {
      return cacheMarshaler;
   }

   /**
    * Caching shortcut for #getComponent(StateTransferManager.class);
    */
   public StateTransferManager getStateTransferManager() {
      return stateTransferManager;
   }

   /**
    * Caching shortcut for #getComponent(ResponseGenerator.class);
    */
   public ResponseGenerator getResponseGenerator() {
      return responseGenerator;
   }

   /**
    * Caching shortcut for #getLocalComponent(CommandsFactory.class);
    */
   public CommandsFactory getCommandsFactory() {
      return commandsFactory;
   }
   /**
    * Caching shortcut for #getComponent(StateTransferManager.class);
    */
   public StateTransferLock getStateTransferLock() {
      return stateTransferLock;
   }


   /**
    * Invoked last after all services are wired
    */
   public void cacheComponents() {
      cacheMarshaler = getOrCreateComponent(StreamingMarshaller.class, KnownComponentNames.CACHE_MARSHALLER);
      stateTransferManager = getOrCreateComponent(StateTransferManager.class);
      responseGenerator = getOrCreateComponent(ResponseGenerator.class);
      commandsFactory = getLocalComponent(CommandsFactory.class);
      totalOrderManager = getOrCreateComponent(TotalOrderManager.class);
      stateTransferLock = getOrCreateComponent(StateTransferLock.class);
   }

   @Override
   public ComponentMetadataRepo getComponentMetadataRepo() {
      return globalComponents.getComponentMetadataRepo();
   }

   public final TotalOrderManager getTotalOrderManager() {
      return totalOrderManager;
   }

}
