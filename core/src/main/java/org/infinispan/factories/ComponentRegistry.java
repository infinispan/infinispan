package org.infinispan.factories;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.CacheConfigurationMBean;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.conflict.impl.StateReceiver;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.components.ComponentMetadata;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.persistence.manager.PreloadManager;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.LocalStreamManager;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;

/**
 * Named cache specific components
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ComponentRegistry extends AbstractComponentRegistry {
   private static final Log log = LogFactory.getLog(ComponentRegistry.class);
   private static final boolean trace = log.isTraceEnabled();

   private final String cacheName;
   private final Configuration configuration;
   private final GlobalComponentRegistry globalComponents;

   @Inject private CacheManagerNotifier cacheManagerNotifier;
   // The modules must be initialized before any cache starts
   @Inject private GlobalComponentRegistry.ModulesOuterLifecycle modulesInitialized;

   //Cached fields:
   private StateTransferManager stateTransferManager;
   private ResponseGenerator responseGenerator;
   private CommandsFactory commandsFactory;
   private StateTransferLock stateTransferLock;
   private PerCacheInboundInvocationHandler inboundInvocationHandler;
   private VersionGenerator versionGenerator;
   private DistributionManager distributionManager;

   /**
    * Creates an instance of the component registry.  The configuration passed in is automatically registered.
    *
    * @param configuration    configuration with which this is created
    * @param cache            cache
    * @param globalComponents Shared Component Registry to delegate to
    */
   public ComponentRegistry(String cacheName, Configuration configuration, AdvancedCache<?, ?> cache,
                            GlobalComponentRegistry globalComponents, ClassLoader defaultClassLoader) {
      super(globalComponents.getComponentMetadataRepo(), defaultClassLoader, Scopes.NAMED_CACHE,
            globalComponents.getComponent(BasicComponentRegistry.class));

      if (cacheName == null) throw new CacheConfigurationException("Cache name cannot be null!");

      try {
         this.cacheName = cacheName;
         this.configuration = configuration;
         this.globalComponents = globalComponents;

         basicComponentRegistry.registerComponent(KnownComponentNames.CACHE_NAME, cacheName, false);
         basicComponentRegistry.registerComponent(ComponentRegistry.class, this, false);
         basicComponentRegistry.registerComponent(Configuration.class, configuration, false);

         bootstrapComponents();
      } catch (Exception e) {
         throw new CacheException("Unable to construct a ComponentRegistry!", e);
      }
   }

   @Override
   protected ClassLoader getClassLoader() {
      return globalComponents.getClassLoader();
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   @SuppressWarnings("unchecked")
   public final <T> T getComponent(String componentTypeName, String name, boolean nameIsFQCN) {
      Class<T> componentType = Util.loadClass(componentTypeName, getClassLoader());
      ComponentRef<T> component = basicComponentRegistry.getComponent(name, componentType);
      return component != null ? component.running() : null;
   }

   @SuppressWarnings("unchecked")
   public final <T> T getLocalComponent(String componentTypeName, String name, boolean nameIsFQCN) {
      Class<T> componentType = Util.loadClass(componentTypeName, getClassLoader());
      ComponentRef<T> componentRef = basicComponentRegistry.getComponent(name, componentType);
      if (componentRef == null || componentRef.wired() == null)
         return null;

      Class<?> componentClass = componentRef.wired().getClass();
      ComponentMetadata metadata = getComponentMetadataRepo().getComponentMetadata(componentClass);
      if (metadata != null && metadata.isGlobalScope())
         return null;

      return componentRef.running();
   }

   @SuppressWarnings("unchecked")
   public final <T> T getLocalComponent(Class<T> componentType) {
      String componentTypeName = componentType.getName();
      return (T) getLocalComponent(componentTypeName, componentTypeName, true);
   }

   protected final Component lookupLocalComponent(String componentClassName, String name, boolean nameIsFQCN) {
      throw new UnsupportedOperationException("The component metadata is no longer exposed");
   }

   public final GlobalComponentRegistry getGlobalComponentRegistry() {
      return globalComponents;
   }

   @Override
   protected final <T> T getOrCreateComponent(Class<T> componentClass, String name, boolean nameIsFQCN) {
      ComponentRef<T> component = basicComponentRegistry.getComponent(name, componentClass);
      return component != null ? component.running() : null;
   }

   @Override
   public void start() {
      // Override AbstractComponentRegistry.start() to allow restarting caches from JMX
      // If FAILED, stop the existing components and transition to TERMINATED
      if (state.needToDestroyFailedCache()) {
         stop();
      }

      // If TERMINATED, rewire non-volatile components and transition to INSTANTIATED
      if (state.needToInitializeBeforeStart()) {
         state = ComponentStatus.INSTANTIATED;

         // TODO Dan: Investigate either re-creating volatile dependencies of non-volatile components automatically
         // in BasicComponentRegistry.start() or removing @SurvivesRestarts altogether.
         rewire();
      }

      super.start();
   }

   @Override
   protected void preStart() {
      // set this up *before* starting the components since some components - specifically state transfer -
      // needs to be able to locate this registry via the InboundInvocationHandler
      cacheComponents();
      this.globalComponents.registerNamedComponentRegistry(this, cacheName);

      notifyCacheStarting(configuration);
   }

   @Override
   protected void postStart() {
      cacheManagerNotifier.notifyCacheStarted(cacheName);
   }

   private void notifyCacheStarting(Configuration configuration) {
      for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
         l.cacheStarting(this, configuration, cacheName);
      }
   }

   @Override
   protected void preStop() {
      // TODO Dan: This should be done by StateTransferManager after sending the leave request
      globalComponents.unregisterNamedComponentRegistry(cacheName);

      for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoking %s.cacheStopping()", l);
         }
         try {
            l.cacheStopping(this, cacheName);
         } catch (Throwable t) {
            log.moduleStopError(l.getClass().getName() + ":" + cacheName, t);
         }
      }
   }

   @Override
   protected void postStop() {
      for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoking %s.cacheStopped()", l);
         }
         try {
            l.cacheStopped(this, cacheName);
         } catch (Throwable t) {
            log.moduleStopError(l.getClass().getName() + ":" + cacheName, t);
         }
      }
      cacheManagerNotifier.notifyCacheStopped(cacheName);
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
      return globalComponents.getComponent(StreamingMarshaller.class);
   }

   /**
    * Caching shortcut for #getComponent(StateTransferManager.class);
    */
   public StateTransferManager getStateTransferManager() {
      return stateTransferManager;
   }

   /**
    * Caching shortcut for #getComponent(DistributionManager.class);
    */
   public DistributionManager getDistributionManager() {
      return distributionManager;
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
    * Caching shortcut for #getLocalComponent(VersionGenerator.class)
    */
   public VersionGenerator getVersionGenerator() {
      return versionGenerator;
   }

   /**
    * Caching shortcut for #getComponent(PerCacheInboundInvocationHandler.class);
    */
   public PerCacheInboundInvocationHandler getPerCacheInboundInvocationHandler() {
      return inboundInvocationHandler;
   }

   /**
    * Invoked before any {@link ModuleCommandInitializer}.
    * This is a good place to register components that don't have any dependency.
    */
   protected void bootstrapComponents() {
   }

   /**
    * Invoked last after all services are wired
    */
   public void cacheComponents() {
      stateTransferManager = basicComponentRegistry.getComponent(StateTransferManager.class).wired();
      responseGenerator = basicComponentRegistry.getComponent(ResponseGenerator.class).wired();
      commandsFactory = basicComponentRegistry.getComponent(CommandsFactory.class).wired();
      stateTransferLock = basicComponentRegistry.getComponent(StateTransferLock.class).wired();
      inboundInvocationHandler = basicComponentRegistry.getComponent(PerCacheInboundInvocationHandler.class).wired();
      versionGenerator = basicComponentRegistry.getComponent(VersionGenerator.class).wired();
      distributionManager = basicComponentRegistry.getComponent(DistributionManager.class).wired();

      // Initialize components that don't have any strong references from the cache
      basicComponentRegistry.getComponent(ClusterCacheStats.class);
      basicComponentRegistry.getComponent(CacheConfigurationMBean.class);
      basicComponentRegistry.getComponent(InternalConflictManager.class);
      basicComponentRegistry.getComponent(LocalStreamManager.class);
      basicComponentRegistry.getComponent(ClusterStreamManager.class);
      basicComponentRegistry.getComponent(XSiteStateTransferManager.class);
      basicComponentRegistry.getComponent(BackupSender.class);
      basicComponentRegistry.getComponent(StateTransferManager.class);
      basicComponentRegistry.getComponent(StateReceiver.class);
      basicComponentRegistry.getComponent(PreloadManager.class);
   }

   @Override
   public ComponentMetadataRepo getComponentMetadataRepo() {
      return globalComponents.getComponentMetadataRepo();
   }

   public final TransactionTable getTransactionTable() {
      return getComponent(org.infinispan.transaction.impl.TransactionTable.class);
   }

   public final synchronized void registerVersionGenerator(NumericVersionGenerator newVersionGenerator) {
      versionGenerator = newVersionGenerator;
      registerComponent(newVersionGenerator, VersionGenerator.class);
   }
}
