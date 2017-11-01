package org.infinispan.factories;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.infinispan.Version;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.uberjar.ManifestUberJarDuplicatedJarsWarner;
import org.infinispan.commons.util.uberjar.UberJarDuplicatedJarsWarner;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.conflict.EntryMergePolicyFactoryRegistry;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
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
import org.infinispan.util.TimeService;
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

   /**
    * Tracking set of created caches in order to make it easy to remove a cache on remote nodes.
    */
   private final Set<String> createdCaches;

   private final ModuleProperties moduleProperties = new ModuleProperties();

   private final ComponentMetadataRepo componentMetadataRepo;

   final Collection<ModuleLifecycle> moduleLifecycles;

   final ConcurrentMap<ByteString, ComponentRegistry> namedComponents = new ConcurrentHashMap<>(4);

   protected final WeakReference<ClassLoader> defaultClassLoader;

   /**
    * Creates an instance of the component registry.  The configuration passed in is automatically registered.
    *
    * @param configuration configuration with which this is created
    */
   public GlobalComponentRegistry(GlobalConfiguration configuration,
                                  EmbeddedCacheManager cacheManager,
                                  Set<String> createdCaches) {
      ClassLoader configuredClassLoader = configuration.classLoader();
      moduleLifecycles = ModuleProperties.resolveModuleLifecycles(configuredClassLoader);

      componentMetadataRepo = new ComponentMetadataRepo();

      // Load up the component metadata
      componentMetadataRepo.initialize(ModuleProperties.getModuleMetadataFiles(configuredClassLoader), configuredClassLoader);

      defaultClassLoader = new WeakReference<>(registerDefaultClassLoader(configuredClassLoader));

      try {
         // this order is important ...
         globalConfiguration = configuration;

         registerComponent(this, GlobalComponentRegistry.class);
         registerComponent(configuration, GlobalConfiguration.class);
         registerComponent(cacheManager, EmbeddedCacheManager.class);
         registerComponent(new CacheManagerJmxRegistration(), CacheManagerJmxRegistration.class);
         registerComponent(new CacheManagerNotifierImpl(), CacheManagerNotifier.class);
         registerComponent(new InternalCacheRegistryImpl(), InternalCacheRegistry.class);
         registerComponent(new CacheStoreFactoryRegistry(), CacheStoreFactoryRegistry.class);
         registerComponent(new EntryMergePolicyFactoryRegistry(), EntryMergePolicyFactoryRegistry.class);
         registerComponent(new GlobalXSiteAdminOperations(), GlobalXSiteAdminOperations.class);

         moduleProperties.loadModuleCommandHandlers(configuredClassLoader);
         Map<Byte, ModuleCommandFactory> factories = moduleProperties.moduleCommandFactories();
         if (factories != null && !factories.isEmpty())
            registerNonVolatileComponent(factories, KnownComponentNames.MODULE_COMMAND_FACTORIES);
         else
            registerNonVolatileComponent(Collections.emptyMap(), KnownComponentNames.MODULE_COMMAND_FACTORIES);
         this.createdCaches = createdCaches;

         getOrCreateComponent(EventLogManager.class);
         // This is necessary to make sure the transport has been started and is available to other components that
         // may need it.  This is a messy approach though - a proper fix will be in ISPN-1698
         getOrCreateComponent(Transport.class);
         // These two should not be necessary, but they are here as a workaround for ISPN-2371
         getOrCreateComponent(LocalTopologyManager.class);
         getOrCreateComponent(ClusterTopologyManager.class);
         getOrCreateComponent(ClusterContainerStats.class);
         getOrCreateComponent(EncoderRegistry.class);
         getOrCreateComponent(GlobalConfigurationManager.class);

         getOrCreateComponent(ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR);
      } catch (Exception e) {
         throw new CacheException("Unable to construct a GlobalComponentRegistry!", e);
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
   public ComponentMetadataRepo getComponentMetadataRepo() {
      return componentMetadataRepo;
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

   @Override
   protected synchronized void addShutdownHook() {
      ArrayList<MBeanServer> al = MBeanServerFactory.findMBeanServer(null);
      ShutdownHookBehavior shutdownHookBehavior = globalConfiguration.shutdown().hookBehavior();
      boolean registerShutdownHook = (shutdownHookBehavior == ShutdownHookBehavior.DEFAULT && al.isEmpty())
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

   public Map<Byte,ModuleCommandInitializer> getModuleCommandInitializers() {
      //moduleProperties is final so we don't need to synchronize this method for safe-publishing
      return Collections.unmodifiableMap(moduleProperties.moduleCommandInitializers());
   }

   @Override
   public void start() {
      try {
         boolean needToNotify;
         synchronized (this) {
            // Do nothing if the global components are already running
            if (!state.startAllowed())
               return;

            needToNotify = state != ComponentStatus.RUNNING && state != ComponentStatus.INITIALIZING;
            if (needToNotify) {
               for (ModuleLifecycle l : moduleLifecycles) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Invoking %s.cacheManagerStarting()", l);
                  }
                  l.cacheManagerStarting(this, globalConfiguration);
               }
            }
            super.start();
         }

         if (versionLogged.compareAndSet(false, true)) {
            log.version(Version.printVersion());
         }

         if (needToNotify && state == ComponentStatus.RUNNING) {
            for (ModuleLifecycle l : moduleLifecycles) {
               if (log.isTraceEnabled()) {
                  log.tracef("Invoking %s.cacheManagerStarted()", l);
               }
               l.cacheManagerStarted(this);
            }
         }

         // Now invoke all post start events
         super.postStart();

         warnAboutUberJarDuplicates();
      } catch (RuntimeException rte) {
         EmbeddedCacheManagerStartupException exception = new EmbeddedCacheManagerStartupException(rte);
         state = ComponentStatus.FAILED;

         try {
            super.stop();
         } catch (Exception e) {
            exception.addSuppressed(e);
         }
         throw exception;
      }
   }

   private void warnAboutUberJarDuplicates() {
      UberJarDuplicatedJarsWarner scanner = new ManifestUberJarDuplicatedJarsWarner();
      scanner.isClasspathCorrectAsync()
            .thenAcceptAsync(isClasspathCorrect -> {
               if(!isClasspathCorrect)
                  log.warnAboutUberJarDuplicates();
            });
   }

   @Override
   public synchronized void stop() {
      boolean needToNotify = state == ComponentStatus.RUNNING || state == ComponentStatus.INITIALIZING;
      if (needToNotify) {
         for (ModuleLifecycle l : moduleLifecycles) {
            if (log.isTraceEnabled()) {
               log.tracef("Invoking %s.cacheManagerStopping()", l);
            }
            l.cacheManagerStopping(this);
         }
      }

      super.stop();

      if (state == ComponentStatus.TERMINATED && needToNotify) {
         for (ModuleLifecycle l : moduleLifecycles) {
            if (log.isTraceEnabled()) {
               log.tracef("Invoking %s.cacheManagerStopped()", l);
            }
            l.cacheManagerStopped(this);
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

   public ModuleProperties getModuleProperties() {
      return moduleProperties;
   }
}
