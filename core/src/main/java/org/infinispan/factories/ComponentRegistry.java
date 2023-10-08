package org.infinispan.factories;

import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.CacheConfigurationMBean;
import org.infinispan.cache.impl.InternalCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.versioning.NumericVersionGenerator;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentAccessor;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.persistence.manager.PreloadManager;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.infinispan.xsite.status.TakeOfflineManager;

/**
 * Named cache specific components
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public class ComponentRegistry extends AbstractComponentRegistry {
   private static final Log log = LogFactory.getLog(ComponentRegistry.class);

   private final String cacheName;
   private final ByteString cacheByteString;
   private final Configuration configuration;
   private final GlobalComponentRegistry globalComponents;

   @Inject
   CacheManagerNotifier cacheManagerNotifier;

   // All modules must be initialized before the first cache starts
   @SuppressWarnings("unused")
   @Inject
   GlobalComponentRegistry.ModuleInitializer moduleInitializer;

   //Cached fields:
   private ComponentRef<AdvancedCache> cache;
   private ComponentRef<AsyncInterceptorChain> asyncInterceptorChain;
   private ComponentRef<BackupReceiver> backupReceiver;
   private ComponentRef<BackupSender> backupSender;
   private ComponentRef<BlockingManager> blockingManager;
   private ComponentRef<ClusterPublisherManager> clusterPublisherManager;
   private ComponentRef<ClusterPublisherManager> localClusterPublisherManager;
   private ComponentRef<CacheNotifier> cacheNotifier;
   private ComponentRef<ClusterCacheNotifier> clusterCacheNotifier;
   private ComponentRef<CommandAckCollector> commandAckCollector;
   private ComponentRef<CommandsFactory> commandsFactory;
   private ComponentRef<InternalConflictManager> conflictManager;
   private ComponentRef<DistributionManager> distributionManager;
   private ComponentRef<InternalDataContainer> internalDataContainer;
   protected ComponentRef<InternalEntryFactory> internalEntryFactory;
   private ComponentRef<InvocationContextFactory> invocationContextFactory;
   private ComponentRef<IracManager> iracManager;
   private ComponentRef<IracVersionGenerator> iracVersionGenerator;
   private ComponentRef<IracTombstoneManager> iracTombstoneCleaner;
   private ComponentRef<LocalPublisherManager> localPublisherManager;
   private ComponentRef<LockManager> lockManager;
   private ComponentRef<PerCacheInboundInvocationHandler> inboundInvocationHandler;
   private ComponentRef<PersistenceMarshaller> persistenceMarshaller;
   private ComponentRef<PublisherHandler> publisherHandler;
   private ComponentRef<RecoveryManager> recoveryManager;
   private ComponentRef<ResponseGenerator> responseGenerator;
   private ComponentRef<RpcManager> rpcManager;
   private ComponentRef<StateTransferLock> stateTransferLock;
   private ComponentRef<StateTransferManager> stateTransferManager;
   private ComponentRef<Marshaller> internalMarshaller;
   private ComponentRef<TakeOfflineManager> takeOfflineManager;
   private ComponentRef<TransactionTable> transactionTable;
   private ComponentRef<VersionGenerator> versionGenerator;
   private ComponentRef<XSiteStateTransferManager> xSiteStateTransferManager;
   private ComponentRef<InternalCacheRegistry> icr;
   // Global, but we don't cache components at global scope
   private TimeService timeService;

   /**
    * Creates an instance of the component registry.  The configuration passed in is automatically registered.
    *
    * @param configuration    configuration with which this is created
    * @param cache            cache
    * @param globalComponents Shared Component Registry to delegate to
    */
   public ComponentRegistry(String cacheName, Configuration configuration, AdvancedCache<?, ?> cache,
                            GlobalComponentRegistry globalComponents, ClassLoader defaultClassLoader) {
      super(globalComponents.moduleRepository,
            false, globalComponents.getComponent(BasicComponentRegistry.class));

      if (cacheName == null) throw new CacheConfigurationException("Cache name cannot be null!");

      try {
         this.cacheName = cacheName;
         this.cacheByteString = ByteString.fromString(cacheName);
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
   public final <T> T getComponent(String componentTypeName, String name, boolean nameIsFQCN) {
      Class<T> componentType = Util.loadClass(componentTypeName, getClassLoader());
      ComponentRef<T> component = basicComponentRegistry.getComponent(name, componentType);
      return component != null ? component.running() : null;
   }

   public final <T> T getLocalComponent(String componentTypeName, String name, boolean nameIsFQCN) {
      Class<T> componentType = Util.loadClass(componentTypeName, getClassLoader());
      ComponentRef<T> componentRef = basicComponentRegistry.getComponent(name, componentType);
      if (componentRef == null || componentRef.wired() == null)
         return null;

      Class<?> componentClass = componentRef.wired().getClass();
      ComponentAccessor<Object> metadata = moduleRepository.getComponentAccessor(componentClass.getName());
      if (metadata != null && metadata.isGlobalScope())
         return null;

      return componentRef.running();
   }

   public final <T> T getLocalComponent(Class<T> componentType) {
      String componentTypeName = componentType.getName();
      return getLocalComponent(componentTypeName, componentTypeName, true);
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

   /**
    * The {@link ComponentRegistry} may delay the start after a cluster shutdown. The component waits for
    * the installation of the previous stable topology to start.
    *
    * Caches that do not need state transfer or are private do not need to delay the start.
    */
   @Override
   protected CompletionStage<Void> delayStart() {
      if (icr == null || icr.wired().isInternalCache(cacheName)) return null;

      synchronized (this) {
         LocalTopologyManager ltm;
         if (state == ComponentStatus.INITIALIZING && (ltm = globalComponents.getLocalTopologyManager()) != null) {
            return ltm.stableTopologyCompletion(cacheName);
         }
      }

      return null;
   }

   @Override
   protected String getName() {
      return "Cache " + cacheName;
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
      CompletionStages.join(cacheManagerNotifier.notifyCacheStarted(cacheName));
   }

   private void notifyCacheStarting(Configuration configuration) {
      for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
         if (log.isTraceEnabled()) {
            log.tracef("Invoking %s.cacheStarting()", l);
         }
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
      CompletionStages.join(cacheManagerNotifier.notifyCacheStopped(cacheName));
   }

   @Override
   public void rewire() {
      super.rewire();
      cacheComponents();
   }

   @Override
   public TimeService getTimeService() {
      return timeService;
   }

   public String getCacheName() {
      return cacheName;
   }

   @Deprecated(forRemoval = true)
   public Marshaller getCacheMarshaller() {
      return internalMarshaller.wired();
   }

   /**
    * Caching shortcut for #getComponent(Marshaller.class, INTERNAL_MARSHALLER);
    */
   public Marshaller getInternalMarshaller() {
      return internalMarshaller.wired();
   }

   /**
    * Caching shortcut for #getComponent(PersistenceMarshaller.class, PERSISTENCE_MARSHALLER);
    */
   public PersistenceMarshaller getPersistenceMarshaller() {
      return persistenceMarshaller.wired();
   }

   /**
    * Caching shortcut for #getComponent(StateTransferManager.class);
    */
   public StateTransferManager getStateTransferManager() {
      return stateTransferManager.wired();
   }

   /**
    * Caching shortcut for #getComponent(DistributionManager.class);
    */
   public DistributionManager getDistributionManager() {
      return distributionManager == null ? null : distributionManager.wired();
   }

   /**
    * Caching shortcut for #getComponent(ResponseGenerator.class);
    */
   public ResponseGenerator getResponseGenerator() {
      return responseGenerator.wired();
   }

   /**
    * Caching shortcut for #getLocalComponent(CommandsFactory.class);
    */
   public CommandsFactory getCommandsFactory() {
      return commandsFactory.wired();
   }

   /**
    * Caching shortcut for #getComponent(StateTransferManager.class);
    */
   public StateTransferLock getStateTransferLock() {
      return stateTransferLock.wired();
   }

   /**
    * Caching shortcut for #getLocalComponent(VersionGenerator.class)
    */
   public VersionGenerator getVersionGenerator() {
      return versionGenerator == null ? null : versionGenerator.wired();
   }

   /**
    * Caching shortcut for #getComponent(PerCacheInboundInvocationHandler.class);
    */
   public PerCacheInboundInvocationHandler getPerCacheInboundInvocationHandler() {
      return inboundInvocationHandler.wired();
   }

   /**
    * This is a good place to register components that don't have any dependency.
    */
   protected void bootstrapComponents() {
   }

   /**
    * Invoked last after all services are wired
    */
   public void cacheComponents() {
      asyncInterceptorChain = basicComponentRegistry.getComponent(AsyncInterceptorChain.class);
      backupReceiver = basicComponentRegistry.lazyGetComponent(BackupReceiver.class); //can we avoid instantiate BackupReceiver is not needed?
      backupSender = basicComponentRegistry.getComponent(BackupSender.class);
      blockingManager = basicComponentRegistry.getComponent(BlockingManager.class);
      clusterPublisherManager = basicComponentRegistry.getComponent(ClusterPublisherManager.class);
      localClusterPublisherManager = basicComponentRegistry.getComponent(PublisherManagerFactory.LOCAL_CLUSTER_PUBLISHER, ClusterPublisherManager.class);
      takeOfflineManager = basicComponentRegistry.getComponent(TakeOfflineManager.class);
      cache = basicComponentRegistry.getComponent(AdvancedCache.class);
      cacheNotifier = basicComponentRegistry.getComponent(CacheNotifier.class);
      conflictManager = basicComponentRegistry.getComponent(InternalConflictManager.class);
      commandsFactory = basicComponentRegistry.getComponent(CommandsFactory.class);
      clusterCacheNotifier = basicComponentRegistry.getComponent(ClusterCacheNotifier.class);
      commandAckCollector = basicComponentRegistry.getComponent(CommandAckCollector.class);
      distributionManager = basicComponentRegistry.getComponent(DistributionManager.class);
      inboundInvocationHandler = basicComponentRegistry.getComponent(PerCacheInboundInvocationHandler.class);
      internalDataContainer = basicComponentRegistry.getComponent(InternalDataContainer.class);
      internalEntryFactory = basicComponentRegistry.getComponent(InternalEntryFactory.class);
      internalMarshaller = basicComponentRegistry.getComponent(KnownComponentNames.INTERNAL_MARSHALLER, Marshaller.class);
      invocationContextFactory = basicComponentRegistry.getComponent(InvocationContextFactory.class);
      iracManager = basicComponentRegistry.getComponent(IracManager.class);
      iracVersionGenerator = basicComponentRegistry.getComponent(IracVersionGenerator.class);
      iracTombstoneCleaner = basicComponentRegistry.getComponent(IracTombstoneManager.class);
      localPublisherManager = basicComponentRegistry.getComponent(LocalPublisherManager.class);
      lockManager = basicComponentRegistry.getComponent(LockManager.class);
      persistenceMarshaller = basicComponentRegistry.getComponent(KnownComponentNames.PERSISTENCE_MARSHALLER, PersistenceMarshaller.class);
      publisherHandler = basicComponentRegistry.getComponent(PublisherHandler.class);
      recoveryManager = basicComponentRegistry.getComponent(RecoveryManager.class);
      responseGenerator = basicComponentRegistry.getComponent(ResponseGenerator.class);
      rpcManager = basicComponentRegistry.getComponent(RpcManager.class);
      stateTransferLock = basicComponentRegistry.getComponent(StateTransferLock.class);
      stateTransferManager = basicComponentRegistry.getComponent(StateTransferManager.class);
      transactionTable = basicComponentRegistry.getComponent(org.infinispan.transaction.impl.TransactionTable.class);
      versionGenerator = basicComponentRegistry.getComponent(VersionGenerator.class);
      xSiteStateTransferManager = basicComponentRegistry.getComponent(XSiteStateTransferManager.class);
      icr = basicComponentRegistry.getComponent(InternalCacheRegistry.class);
      timeService = basicComponentRegistry.getComponent(TimeService.class).running();

      // Initialize components that don't have any strong references from the cache
      basicComponentRegistry.getComponent(ClusterCacheStats.class);
      basicComponentRegistry.getComponent(CacheConfigurationMBean.class);
      basicComponentRegistry.getComponent(InternalConflictManager.class);
      basicComponentRegistry.getComponent(PreloadManager.class);
   }

   public final TransactionTable getTransactionTable() {
      return transactionTable.wired();
   }

   public final ComponentRef<TransactionTable> getTransactionTableRef() {
      return transactionTable;
   }


   public final synchronized void registerVersionGenerator(NumericVersionGenerator newVersionGenerator) {
      registerComponent(newVersionGenerator, VersionGenerator.class);
      versionGenerator = basicComponentRegistry.getComponent(VersionGenerator.class);
   }

   public ComponentRef<AdvancedCache> getCache() {
      return cache;
   }

   public ComponentRef<AsyncInterceptorChain> getInterceptorChain() {
      return asyncInterceptorChain;
   }

   public ComponentRef<BackupSender> getBackupSender() {
      return backupSender;
   }

   public ComponentRef<BlockingManager> getBlockingManager() {
      return blockingManager;
   }

   public ComponentRef<ClusterPublisherManager> getClusterPublisherManager() {
      return clusterPublisherManager;
   }

   public ComponentRef<ClusterPublisherManager> getLocalClusterPublisherManager() {
      return localClusterPublisherManager;
   }

   public ComponentRef<TakeOfflineManager> getTakeOfflineManager() {
      return takeOfflineManager;
   }

   public ComponentRef<IracManager> getIracManager() {
      return iracManager;
   }

   public ComponentRef<IracVersionGenerator> getIracVersionGenerator() {
      return iracVersionGenerator;
   }

   public ComponentRef<IracTombstoneManager> getIracTombstoneManager() {
      return iracTombstoneCleaner;
   }

   public ByteString getCacheByteString() {
      return cacheByteString;
   }

   public ComponentRef<CacheNotifier> getCacheNotifier() {
      return cacheNotifier;
   }

   public Configuration getConfiguration() {
      return configuration;
   }

   public ComponentRef<InternalConflictManager> getConflictManager() {
      return conflictManager;
   }

   public ComponentRef<ClusterCacheNotifier> getClusterCacheNotifier() {
      return clusterCacheNotifier;
   }

   public ComponentRef<CommandAckCollector> getCommandAckCollector() {
      return commandAckCollector;
   }

   public ComponentRef<InternalDataContainer> getInternalDataContainer() {
      return internalDataContainer;
   }

   public ComponentRef<InternalEntryFactory> getInternalEntryFactory() {
      return internalEntryFactory;
   }

   public ComponentRef<InvocationContextFactory> getInvocationContextFactory() {
      return invocationContextFactory;
   }

   public ComponentRef<LocalPublisherManager> getLocalPublisherManager() {
      return localPublisherManager;
   }

   public ComponentRef<PublisherHandler> getPublisherHandler() {
      return publisherHandler;
   }

   public ComponentRef<LockManager> getLockManager() {
      return lockManager;
   }

   public ComponentRef<RecoveryManager> getRecoveryManager() {
      return recoveryManager;
   }

   public ComponentRef<RpcManager> getRpcManager() {
      return rpcManager;
   }

   public ComponentRef<XSiteStateTransferManager> getXSiteStateTransferManager() {
      return xSiteStateTransferManager;
   }

   public ComponentRef<BackupReceiver> getBackupReceiver() {
      return backupReceiver;
   }

   public static <K, V> ComponentRegistry of(Cache<K, V> cache) {
      return ((InternalCache<K, V>) cache).getComponentRegistry();
   }

   public static <T> T componentOf(Cache<?, ?> cache, Class<T> type) {
      return of(cache).getComponent(type);
   }

   public static <T> T componentOf(Cache<?, ?> cache, Class<T> type, String name) {
      return of(cache).getComponent(type, name);
   }

}
