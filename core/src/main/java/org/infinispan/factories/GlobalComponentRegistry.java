package org.infinispan.factories;

import static org.infinispan.factories.KnownComponentNames.MODULE_COMMAND_INITIALIZERS;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServerFactory;

import org.infinispan.commons.util.Version;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.conflict.EntryMergePolicyFactoryRegistry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.metrics.impl.CacheManagerMetricsRegistration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.ModuleRepository;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifierImpl;
import org.infinispan.persistence.factory.CacheStoreFactoryRegistry;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.registry.impl.InternalCacheRegistryImpl;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.stats.ClusterContainerStats;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.ModuleProperties;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.xsite.GlobalXSiteAdminOperations;

import net.jcip.annotations.ThreadSafe;

/**
 * A global component registry where shared components are stored.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
@ThreadSafe
public class GlobalComponentRegistry extends AbstractComponentRegistry {

   private static final Log log = LogFactory.getLog(GlobalComponentRegistry.class);

   private static final AtomicBoolean versionLogged = new AtomicBoolean(false);

   /**
    * Hook to shut down the cache when the JVM exits.
    */
   private Thread shutdownHook;
   /**
    * A flag that the shutdown hook sets before calling cache.stop().  Allows stop() to identify if it has been called
    * from a shutdown hook.
    */
   private boolean invokedFromShutdownHook;

   private final GlobalConfiguration globalConfiguration;

   private final EmbeddedCacheManager cacheManager;
   /**
    * Tracking set of created caches in order to make it easy to remove a cache on remote nodes.
    */
   private final Set<String> createdCaches;

   private final ModuleProperties moduleProperties = new ModuleProperties();

   final Collection<ModuleLifecycle> moduleLifecycles;

   private final ConcurrentMap<ByteString, ComponentRegistry> namedComponents = new ConcurrentHashMap<>(4);

   protected final ClassLoader classLoader;

   /**
    * Creates an instance of the component registry.  The configuration passed in is automatically registered.
    *
    * @param configuration configuration with which this is created
    * @param configurationManager
    */
   public GlobalComponentRegistry(GlobalConfiguration configuration,
                                  EmbeddedCacheManager cacheManager,
                                  Set<String> createdCaches, ModuleRepository moduleRepository,
                                  ConfigurationManager configurationManager) {
      super(moduleRepository, true, null);

      ClassLoader configuredClassLoader = configuration.classLoader();
      moduleLifecycles = moduleRepository.getModuleLifecycles();

      classLoader = configuredClassLoader;

      try {
         // this order is important ...
         globalConfiguration = configuration;

         registerComponent(this, GlobalComponentRegistry.class);
         registerComponent(configuration, GlobalConfiguration.class);
         registerComponent(cacheManager, EmbeddedCacheManager.class);
         basicComponentRegistry.registerComponent(ConfigurationManager.class.getName(), configurationManager, true);
         basicComponentRegistry.registerComponent(CacheManagerJmxRegistration.class.getName(), new CacheManagerJmxRegistration(), true);
         basicComponentRegistry.registerComponent(CacheManagerMetricsRegistration.class.getName(), new CacheManagerMetricsRegistration(), true);
         basicComponentRegistry.registerComponent(CacheManagerNotifier.class.getName(), new CacheManagerNotifierImpl(), true);
         basicComponentRegistry.registerComponent(InternalCacheRegistry.class.getName(), new InternalCacheRegistryImpl(), true);
         basicComponentRegistry.registerComponent(CacheStoreFactoryRegistry.class.getName(), new CacheStoreFactoryRegistry(), true);
         basicComponentRegistry.registerComponent(EntryMergePolicyFactoryRegistry.class.getName(), new EntryMergePolicyFactoryRegistry(), true);
         basicComponentRegistry.registerComponent(GlobalXSiteAdminOperations.class.getName(), new GlobalXSiteAdminOperations(), true);

         moduleProperties.loadModuleCommandHandlers(configuredClassLoader);
         Map<Byte, ModuleCommandFactory> factories = moduleProperties.moduleCommandFactories();
         if (factories != null && !factories.isEmpty()) {
            registerNonVolatileComponent(factories, KnownComponentNames.MODULE_COMMAND_FACTORIES);
         } else {
            registerNonVolatileComponent(Collections.emptyMap(), KnownComponentNames.MODULE_COMMAND_FACTORIES);
         }

         // register any module-specific command initializers
         // Modules are on the same classloader as Infinispan
         Map<Byte, ModuleCommandInitializer> initializers = moduleProperties.moduleCommandInitializers();
         if (initializers != null && !initializers.isEmpty()) {
            registerNonVolatileComponent(initializers, MODULE_COMMAND_INITIALIZERS);
            for (ModuleCommandInitializer mci : initializers.values()) {
               if (basicComponentRegistry.getComponent(mci.getClass()) == null) {
                  basicComponentRegistry.registerComponent(mci.getClass(), mci, false);
               }
            }
         } else {
            registerNonVolatileComponent(
               Collections.emptyMap(), MODULE_COMMAND_INITIALIZERS);
         }

         // Allow caches to depend only on module initialization instead of the entire GCR
         basicComponentRegistry.registerComponent(ModuleInitializer.class, new ModuleInitializer(), true);

         this.createdCaches = createdCaches;
         this.cacheManager = cacheManager;

         // Initialize components that do not have strong references from the cache manager
         basicComponentRegistry.getComponent(EventLogManager.class);
         basicComponentRegistry.getComponent(Transport.class);
         basicComponentRegistry.getComponent(LocalTopologyManager.class);
         basicComponentRegistry.getComponent(ClusterTopologyManager.class);
         basicComponentRegistry.getComponent(ClusterContainerStats.class);
         basicComponentRegistry.getComponent(EncoderRegistry.class);
         basicComponentRegistry.getComponent(GlobalConfigurationManager.class);
         basicComponentRegistry.getComponent(CacheManagerJmxRegistration.class);
         basicComponentRegistry.getComponent(CacheManagerMetricsRegistration.class);

         basicComponentRegistry.getComponent(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR, ScheduledExecutorService.class);
      } catch (Exception e) {
         throw new CacheException("Unable to construct a GlobalComponentRegistry!", e);
      }
   }

   @Override
   protected ClassLoader getClassLoader() {
      return classLoader;
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected synchronized void removeShutdownHook() {
      // if this is called from a source other than the shutdown hook, de-register the shutdown hook.
      if (!invokedFromShutdownHook && shutdownHook != null) Runtime.getRuntime().removeShutdownHook(shutdownHook);
   }

   @Override
   public TimeService getTimeService() {
      return getOrCreateComponent(TimeService.class);
   }

   /**
    * This method returns true if there is an mbean server running
    * <p>
    * NOTE: This method is here for Quarkus (due to mbean usage) - so do not remove without modifying Quarkus as well
    * @return true if any mbean server is running
    */
   private boolean isMBeanServerRunning() {
      return !MBeanServerFactory.findMBeanServer(null).isEmpty();
   }

   @Override
   protected synchronized void addShutdownHook() {
      ShutdownHookBehavior shutdownHookBehavior = globalConfiguration.shutdown().hookBehavior();
      boolean registerShutdownHook = (shutdownHookBehavior == ShutdownHookBehavior.DEFAULT && !isMBeanServerRunning())
            || shutdownHookBehavior == ShutdownHookBehavior.REGISTER;

      if (registerShutdownHook) {
         log.tracef("Registering a shutdown hook.  Configured behavior = %s", shutdownHookBehavior);
         shutdownHook = new Thread(() -> {
            try {
               invokedFromShutdownHook = true;
               GlobalComponentRegistry.this.stop();
            } finally {
               invokedFromShutdownHook = false;
            }
         });

         Runtime.getRuntime().addShutdownHook(shutdownHook);
      } else {

         log.tracef("Not registering a shutdown hook.  Configured behavior = %s", shutdownHookBehavior);
      }
   }

   public final ComponentRegistry getNamedComponentRegistry(String name) {
      //no need so sync this method as namedComponents is thread safe and correctly published (final)
      return getNamedComponentRegistry(ByteString.fromString(name));
   }

   public final ComponentRegistry getNamedComponentRegistry(ByteString name) {
      //no need so sync this method as namedComponents is thread safe and correctly published (final)
      return namedComponents.get(name);
   }

   public synchronized final void registerNamedComponentRegistry(ComponentRegistry componentRegistry, String name) {
      namedComponents.put(ByteString.fromString(name), componentRegistry);
   }

   public synchronized final void unregisterNamedComponentRegistry(String name) {
      namedComponents.remove(ByteString.fromString(name));
   }

   public synchronized final void rewireNamedRegistries() {
      for (ComponentRegistry cr : namedComponents.values())
         cr.rewire();
   }

   @Override
   protected void preStart() {
      basicComponentRegistry.getComponent(ModuleInitializer.class).running();

      if (versionLogged.compareAndSet(false, true)) {
         CONTAINER.version(Version.printVersion());
      }
   }

   @Override
   protected void postStart() {
      modulesManagerStarted();
   }

   private void modulesManagerStarting() {
      for (ModuleLifecycle l : moduleLifecycles) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoking %s.cacheManagerStarting()", l);
         }
         l.cacheManagerStarting(this, globalConfiguration);
      }
   }

   private void modulesManagerStarted() {
      for (ModuleLifecycle l : moduleLifecycles) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoking %s.cacheManagerStarted()", l);
         }
         l.cacheManagerStarted(this);
      }
   }

   @Override
   protected void preStop() {
      modulesManagerStopping();
   }

   @Override
   protected void postStop() {
      // Do nothing, ModulesOuterLifecycle invokes modulesManagerStopped automatically
   }

   private void modulesManagerStopping() {
      for (ModuleLifecycle l : moduleLifecycles) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoking %s.cacheManagerStopping()", l);
         }
         try {
            l.cacheManagerStopping(this);
         } catch (Throwable t) {
            CONTAINER.moduleStopError(l.getClass().getName(), t);
         }
      }
   }

   private void modulesManagerStopped() {
      if (state == ComponentStatus.TERMINATED) {
         for (ModuleLifecycle l : moduleLifecycles) {
            if (log.isTraceEnabled()) {
               log.tracef("Invoking %s.cacheManagerStopped()", l);
            }
            try {
               l.cacheManagerStopped(this);
            } catch (Throwable t) {
               CONTAINER.moduleStopError(l.getClass().getName(), t);
            }
         }
      }
   }

   public void notifyCacheStarted(String cacheName) {
      ComponentRegistry cr = getNamedComponentRegistry(cacheName);
      for (ModuleLifecycle l : moduleLifecycles) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoking %s.cacheStarted()", l);
         }
         l.cacheStarted(cr, cacheName);
      }
   }

   public final GlobalConfiguration getGlobalConfiguration() {
      //this is final so no need to synchronise it
      return globalConfiguration;
   }

   /**
    * Removes a cache with the given name, returning true if the cache was removed.
    */
   public synchronized boolean removeCache(String cacheName) {
      return createdCaches.remove(cacheName);
   }

   @Deprecated
   public ModuleProperties getModuleProperties() {
      return moduleProperties;
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   /**
    * Module initialization happens in {@link ModuleLifecycle#cacheManagerStarting(GlobalComponentRegistry, GlobalConfiguration)}.
    * This component helps guarantee that all modules are initialized before the first cache starts.
    */
   @Scope(Scopes.GLOBAL)
   public class ModuleInitializer {
      @Start
      void start() {
         modulesManagerStarting();
      }

      @Stop
      void stop() {
         modulesManagerStopped();
      }
   }
}
