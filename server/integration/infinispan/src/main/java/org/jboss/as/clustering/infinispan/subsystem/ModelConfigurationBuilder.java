package org.jboss.as.clustering.infinispan.subsystem;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.configuration.cache.SitesConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.AbstractJdbcStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.CompressionType;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.rest.configuration.RestStoreConfigurationBuilder;
import org.infinispan.persistence.rest.metadata.MimeMetadataHelper;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.jboss.as.clustering.infinispan.InfinispanMessages;
import org.jboss.as.clustering.infinispan.cs.configuration.DeployedStoreConfigurationBuilder;
import org.jboss.as.clustering.infinispan.subsystem.CacheAdd.Dependency;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.controller.services.path.PathManagerService;
import org.jboss.as.network.OutboundSocketBinding;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.Property;
import org.jboss.logging.Logger;
import org.jboss.msc.inject.Injector;
import org.jboss.msc.service.ServiceName;

/**
 * ModelConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class ModelConfigurationBuilder {
    private static final Logger log = Logger.getLogger(ModelConfigurationBuilder.class.getPackage().getName());
    private static final String DEFAULTS = "infinispan-defaults.xml";
    private static volatile Map<CacheMode, Configuration> defaults = null;

    private static final String[] loaderKeys = new String[] { ModelKeys.LOADER, ModelKeys.CLUSTER_LOADER };
    private static final String[] storeKeys = new String[] { ModelKeys.STORE, ModelKeys.FILE_STORE,
            ModelKeys.STRING_KEYED_JDBC_STORE, ModelKeys.BINARY_KEYED_JDBC_STORE, ModelKeys.MIXED_KEYED_JDBC_STORE,
            ModelKeys.REMOTE_STORE, ModelKeys.LEVELDB_STORE, ModelKeys.REST_STORE };

    private final CacheMode mode;
    private final CacheConfigurationDependencies cacheConfigurationDependencies;

    public ModelConfigurationBuilder(CacheMode mode) {
        this.mode = mode;
        this.cacheConfigurationDependencies = new CacheConfigurationDependencies();
    }

    public CacheMode getMode() {
        return mode;
    }

    public CacheConfigurationDependencies getCacheConfigurationDependencies() {
        return cacheConfigurationDependencies;
    }

    public ConfigurationBuilder processModelNode(OperationContext context, String containerName, ModelNode cache, List<Dependency<?>> dependencies) throws OperationFailedException {
        ConfigurationBuilder builder = new ConfigurationBuilder().read(getDefaultConfiguration(mode));
        switch(mode) {
        case DIST_ASYNC:
        case DIST_SYNC:
            processDistributedCacheConfigurationModel(context, containerName, cache, builder, dependencies);
            break;
        case REPL_ASYNC:
        case REPL_SYNC:
            processReplicatedCacheConfigurationModel(context, containerName, cache, builder, dependencies);
            break;
        case INVALIDATION_ASYNC:
        case INVALIDATION_SYNC:
            processInvalidationCacheConfigurationModel(context, containerName, cache, builder, dependencies);
            break;
        case LOCAL:
            processLocalCacheConfigurationModel(context, containerName, cache, builder, dependencies);
            break;
        }
        return builder;
    }

    private void processLocalCacheConfigurationModel(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies) throws OperationFailedException {
        processCacheConfigurationModel(context, containerName, cache, builder, dependencies);
    }

    private void processDistributedCacheConfigurationModel(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException {
        processSharedStateCacheConfigurationModel(context, containerName, cache, builder, dependencies);

        final int owners = DistributedCacheResource.OWNERS.resolveModelAttribute(context, cache).asInt();
        final int segments = DistributedCacheResource.SEGMENTS.resolveModelAttribute(context, cache).asInt();
        final float capacityFactor = (float) DistributedCacheResource.CAPACITY_FACTOR.resolveModelAttribute(context,
              cache).asDouble();
        final long lifespan = DistributedCacheResource.L1_LIFESPAN.resolveModelAttribute(context, cache).asLong();

        // process the additional distributed attributes and elements
        builder.clustering().hash()
            .numOwners(owners)
            .numSegments(segments)
            .capacityFactor(capacityFactor)
        ;

        if (lifespan > 0) {
            // is disabled by default in L1ConfigurationBuilder
            builder.clustering().l1().enable().lifespan(lifespan);
        } else {
            builder.clustering().l1().disable();
        }
    }

    private void processReplicatedCacheConfigurationModel(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException {
        processSharedStateCacheConfigurationModel(context, containerName, cache, builder, dependencies);
    }

    private void processInvalidationCacheConfigurationModel(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException {
        processClusteredCacheConfigurationModel(context, containerName, cache, builder, dependencies);
    }

    private void processSharedStateCacheConfigurationModel(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException{

        // process the basic clustered configuration
        processClusteredCacheConfigurationModel(context, containerName, cache, builder, dependencies);

        // state transfer is a child resource
        if (cache.hasDefined(ModelKeys.STATE_TRANSFER) && cache.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME).isDefined()) {
            ModelNode stateTransfer = cache.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME);

            final boolean enabled = StateTransferResource.ENABLED.resolveModelAttribute(context, stateTransfer).asBoolean();
            final boolean awaitInitialTransfer = StateTransferResource.AWAIT_INITIAL_TRANSFER.resolveModelAttribute(context,  stateTransfer).asBoolean();
            final long timeout = StateTransferResource.TIMEOUT.resolveModelAttribute(context, stateTransfer).asLong();
            final int chunkSize = StateTransferResource.CHUNK_SIZE.resolveModelAttribute(context, stateTransfer).asInt();

            builder.clustering().stateTransfer().fetchInMemoryState(enabled);
            builder.clustering().stateTransfer().awaitInitialTransfer(awaitInitialTransfer);
            builder.clustering().stateTransfer().timeout(timeout);
            builder.clustering().stateTransfer().chunkSize(chunkSize);
        }

       if (cache.hasDefined(ModelKeys.PARTITION_HANDLING) && cache.get(ModelKeys.PARTITION_HANDLING, ModelKeys.PARTITION_HANDLING_NAME).isDefined()) {
          ModelNode partitionHandling = cache.get(ModelKeys.PARTITION_HANDLING, ModelKeys.PARTITION_HANDLING_NAME);

          final boolean enabled = PartitionHandlingResource.ENABLED.resolveModelAttribute(context, partitionHandling).asBoolean();

          builder.clustering().partitionHandling().enabled(enabled);
       }
    }

    private void processClusteredCacheConfigurationModel(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies) throws OperationFailedException {
        // process the basic configuration
        processCacheConfigurationModel(context, containerName, cache, builder, dependencies);

        // required attribute MODE (ASYNC/SYNC)
        final Mode modelMode = Mode.valueOf(ClusteredCacheResource.MODE.resolveModelAttribute(context, cache).asString()) ;

        final long remoteTimeout = ClusteredCacheResource.REMOTE_TIMEOUT.resolveModelAttribute(context, cache).asLong();
        final int queueSize = ClusteredCacheResource.QUEUE_SIZE.resolveModelAttribute(context, cache).asInt();
        final long queueFlushInterval = ClusteredCacheResource.QUEUE_FLUSH_INTERVAL.resolveModelAttribute(context, cache).asLong();
        final boolean asyncMarshalling = ClusteredCacheResource.ASYNC_MARSHALLING.resolveModelAttribute(context, cache).asBoolean();

        // adjust the cache mode used based on the value of clustered attribute MODE
        CacheMode cacheMode = modelMode.apply(mode);
        builder.clustering().cacheMode(cacheMode);

        // process clustered cache attributes and elements
        if (cacheMode.isSynchronous()) {
            builder.clustering().sync().replTimeout(remoteTimeout);
        } else {
            builder.clustering().async().useReplQueue(queueSize > 0);
            builder.clustering().async().replQueueMaxElements(queueSize);
            builder.clustering().async().replQueueInterval(queueFlushInterval);
            if(asyncMarshalling)
                builder.clustering().async().asyncMarshalling();
            else
                builder.clustering().async().syncMarshalling();
        }
    }

    /**
     * Create a Configuration object initialized from the operation ModelNode
     *
     * @param containerName the name of the cache container
     * @param cache         ModelNode representing cache configuration
     * @param builder       ConfigurationBuilder object to add data to
     * @return initialised Configuration object
     */
    private void processCacheConfigurationModel(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException {
        if (cache.hasDefined(ModelKeys.CONFIGURATION)) {
            String template = CacheResource.CONFIGURATION.resolveModelAttribute(context, cache).asString();
            dependencies.add(new Dependency<>(CacheConfigurationService.getServiceName(containerName, template), Configuration.class, cacheConfigurationDependencies.getParentConfigurationInjector()));
        }

        builder.jmxStatistics().enabled(CacheResource.STATISTICS.resolveModelAttribute(context, cache).asBoolean());

        final Indexing indexing = Indexing.valueOf(CacheResource.INDEXING.resolveModelAttribute(context, cache).asString());
        final boolean autoConfig = CacheResource.INDEXING_AUTO_CONFIG.resolveModelAttribute(context, cache).asBoolean();
        final boolean batching = CacheResource.BATCHING.resolveModelAttribute(context, cache).asBoolean();

        // set the cache mode (may be modified when setting up clustering attributes)
        builder.clustering().cacheMode(this.mode);
        final ModelNode indexingPropertiesModel = CacheResource.INDEXING_PROPERTIES.resolveModelAttribute(context, cache);
        Properties indexingProperties = new Properties();
        if (indexing.isEnabled() && indexingPropertiesModel.isDefined()) {
            for (Property p : indexingPropertiesModel.asPropertyList()) {
                String value = p.getValue().asString();
                indexingProperties.put(p.getName(), value);
            }
        }
        builder.indexing()
                .index(indexing.isEnabled() ? indexing.isLocalOnly() ? Index.LOCAL : Index.ALL : Index.NONE)
                .withProperties(indexingProperties)
                .autoConfig(autoConfig)
        ;

        if (cache.hasDefined(ModelKeys.REMOTE_CACHE)) {
             builder.sites().backupFor().remoteCache(CacheResource.REMOTE_CACHE.resolveModelAttribute(context, cache).asString());
        }
        if (cache.hasDefined(ModelKeys.REMOTE_SITE)) {
             builder.sites().backupFor().remoteSite(CacheResource.REMOTE_SITE.resolveModelAttribute(context, cache).asString());
        }


        // locking is a child resource
        if (cache.hasDefined(ModelKeys.LOCKING) && cache.get(ModelKeys.LOCKING, ModelKeys.LOCKING_NAME).isDefined()) {
            ModelNode locking = cache.get(ModelKeys.LOCKING, ModelKeys.LOCKING_NAME);

            final IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
            if (locking.hasDefined(ModelKeys.ISOLATION)) {
               log.warn("Ignoring XML attribute " + ModelKeys.ISOLATION + ", please remove from configuration file");
            }
            final boolean striping = LockingResource.STRIPING.resolveModelAttribute(context, locking).asBoolean();
            final long acquireTimeout = LockingResource.ACQUIRE_TIMEOUT.resolveModelAttribute(context, locking).asLong();
            final int concurrencyLevel = LockingResource.CONCURRENCY_LEVEL.resolveModelAttribute(context, locking).asInt();

            builder.locking()
                    .isolationLevel(isolationLevel)
                    .useLockStriping(striping)
                    .lockAcquisitionTimeout(acquireTimeout)
                    .concurrencyLevel(concurrencyLevel)
            ;
        }

        TransactionMode txMode = TransactionMode.NONE;
        LockingMode lockingMode = LockingMode.OPTIMISTIC;
        // locking is a child resource
        if (cache.hasDefined(ModelKeys.TRANSACTION) && cache.get(ModelKeys.TRANSACTION, ModelKeys.TRANSACTION_NAME).isDefined()) {
            ModelNode transaction = cache.get(ModelKeys.TRANSACTION, ModelKeys.TRANSACTION_NAME);

            final long stopTimeout = TransactionResource.STOP_TIMEOUT.resolveModelAttribute(context, transaction).asLong();
            txMode = TransactionMode.valueOf(TransactionResource.MODE.resolveModelAttribute(context, transaction).asString());
            lockingMode = LockingMode.valueOf(TransactionResource.LOCKING.resolveModelAttribute(context, transaction).asString());

            builder.transaction().cacheStopTimeout(stopTimeout);
        }
        builder.transaction()
                .transactionMode(txMode.getMode())
                .lockingMode(lockingMode)
                .useSynchronization(!txMode.isXAEnabled())
                .recovery().enabled(txMode.isRecoveryEnabled())
        ;
        if (txMode.isRecoveryEnabled()) {
            builder.transaction().syncCommitPhase(true).syncRollbackPhase(true);
        }

        if (batching) {
            builder.transaction().transactionMode(org.infinispan.transaction.TransactionMode.TRANSACTIONAL).invocationBatching().enable();
        } else {
            builder.transaction().invocationBatching().disable();
        }

        // eviction is a child resource
        if (cache.hasDefined(ModelKeys.EVICTION) && cache.get(ModelKeys.EVICTION, ModelKeys.EVICTION_NAME).isDefined()) {
            ModelNode eviction = cache.get(ModelKeys.EVICTION, ModelKeys.EVICTION_NAME);

            final EvictionStrategy strategy = EvictionStrategy.valueOf(EvictionResource.EVICTION_STRATEGY.resolveModelAttribute(context, eviction).asString());
            builder.eviction().strategy(strategy);

            if (strategy.isEnabled()) {
                final long maxEntries = EvictionResource.MAX_ENTRIES.resolveModelAttribute(context, eviction).asLong();
                builder.eviction().maxEntries(maxEntries);
            }
        }
        // expiration is a child resource
        if (cache.hasDefined(ModelKeys.EXPIRATION) && cache.get(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME).isDefined()) {

            ModelNode expiration = cache.get(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);

            final long maxIdle = ExpirationResource.MAX_IDLE.resolveModelAttribute(context, expiration).asLong();
            final long lifespan = ExpirationResource.LIFESPAN.resolveModelAttribute(context, expiration).asLong();
            final long interval = ExpirationResource.INTERVAL.resolveModelAttribute(context, expiration).asLong();

            builder.expiration()
                    .maxIdle(maxIdle)
                    .lifespan(lifespan)
                    .wakeUpInterval(interval)
            ;
            // Only enable the reaper thread if we need it
            if (interval > 0) {
                builder.expiration().enableReaper();
            } else {
                builder.expiration().disableReaper();
            }
        }

        // compatibility is a child resource
        if (cache.hasDefined(ModelKeys.COMPATIBILITY) && cache.get(ModelKeys.COMPATIBILITY, ModelKeys.COMPATIBILITY_NAME).isDefined()) {

            ModelNode compatibility = cache.get(ModelKeys.COMPATIBILITY, ModelKeys.COMPATIBILITY_NAME);

            final boolean enabled = CompatibilityResource.ENABLED.resolveModelAttribute(context, compatibility).asBoolean();

            builder.compatibility().enabled(enabled);

            if (compatibility.hasDefined(ModelKeys.MARSHALLER)) {
                String marshaller = CompatibilityResource.MARSHALLER.resolveModelAttribute(context, compatibility).asString();
                try {
                    Class<? extends Marshaller> marshallerClass = CacheLoader.class.getClassLoader().loadClass(marshaller).asSubclass(Marshaller.class);
                    builder.compatibility().marshaller(marshallerClass.newInstance());
                } catch (Exception e) {
                    throw InfinispanMessages.MESSAGES.invalidCompatibilityMarshaller(e, marshaller);
                }
            }
        }

        if (cache.hasDefined(ModelKeys.SECURITY) && cache.get(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME).isDefined()) {
            ModelNode securityModel = cache.get(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME);

            if (securityModel.hasDefined(ModelKeys.AUTHORIZATION) && securityModel.get(ModelKeys.AUTHORIZATION).hasDefined(ModelKeys.AUTHORIZATION_NAME)) {
                ModelNode authzModel = securityModel.get(ModelKeys.AUTHORIZATION, ModelKeys.AUTHORIZATION_NAME);

                AuthorizationConfigurationBuilder authzBuilder = builder.security().authorization();
                authzBuilder.enabled(CacheAuthorizationResource.ENABLED.resolveModelAttribute(context, authzModel).asBoolean());
                for(ModelNode role : CacheAuthorizationResource.ROLES.resolveModelAttribute(context, authzModel).asList()) {
                    authzBuilder.role(role.asString());
                }
            }
        }

        // loaders are a child resource
        for (String loaderKey : loaderKeys) {
            handleLoaderProperties(context, cache, loaderKey, containerName, builder, dependencies);
        }

        // stores are a child resource
        for (String storeKey : storeKeys) {
            handleStoreProperties(context, cache, storeKey, containerName, builder, dependencies);
        }

        if (cache.hasDefined(ModelKeys.BACKUP)) {
            SitesConfigurationBuilder sitesBuilder = builder.sites();
            for (Property property : cache.get(ModelKeys.BACKUP).asPropertyList()) {
                String siteName = property.getName();
                ModelNode site = property.getValue();
                BackupConfigurationBuilder backupConfigurationBuilder = sitesBuilder.addBackup();
                backupConfigurationBuilder.site(siteName)
                        .backupFailurePolicy(BackupFailurePolicy.valueOf(BackupSiteResource.FAILURE_POLICY.resolveModelAttribute(context, site).asString()))
                        .strategy(BackupStrategy.valueOf(BackupSiteResource.STRATEGY.resolveModelAttribute(context, site).asString()))
                        .replicationTimeout(BackupSiteResource.REPLICATION_TIMEOUT.resolveModelAttribute(context, site).asLong());
                if (BackupSiteResource.ENABLED.resolveModelAttribute(context, site).asBoolean()) {
                    sitesBuilder.addInUseBackupSite(siteName);
                }
                if (site.hasDefined(ModelKeys.TAKE_OFFLINE)) {
                    ModelNode takeOfflineModel = site.get(ModelKeys.TAKE_OFFLINE);
                    backupConfigurationBuilder.takeOffline()
                          .afterFailures(BackupSiteResource.TAKE_OFFLINE_AFTER_FAILURES.resolveModelAttribute(context, takeOfflineModel).asInt())
                          .minTimeToWait(BackupSiteResource.TAKE_OFFLINE_MIN_WAIT.resolveModelAttribute(context, takeOfflineModel).asLong());
                }
                if (site.hasDefined(ModelKeys.STATE_TRANSFER) && site.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME).isDefined()) {
                    ModelNode stateTransferModel = site.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME);
                    backupConfigurationBuilder.stateTransfer()
                          .chunkSize(BackupSiteStateTransferResource.STATE_TRANSFER_CHUNK_SIZE.resolveModelAttribute(context, stateTransferModel).asInt())
                          .timeout(BackupSiteStateTransferResource.STATE_TRANSFER_TIMEOUT.resolveModelAttribute(context, stateTransferModel).asLong())
                          .maxRetries(BackupSiteStateTransferResource.STATE_TRANSFER_MAX_RETRIES.resolveModelAttribute(context, stateTransferModel).asInt())
                          .waitTime(BackupSiteStateTransferResource.STATE_TRANSFER_WAIT_TIME.resolveModelAttribute(context, stateTransferModel).asLong());
                }

            }
        }
    }

    private void handleLoaderProperties(OperationContext context, ModelNode cache, String loaderKey, String containerName,
                                        ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException {
        if (cache.hasDefined(loaderKey)) {
            for (Property loaderEntry : cache.get(loaderKey).asPropertyList()) {
                ModelNode loader = loaderEntry.getValue();
                PersistenceConfigurationBuilder persistence = builder.persistence();
                StoreConfigurationBuilder<?, ?> scb = buildCacheLoader(persistence,
                                                                       loader, loaderKey);
                parseCommonAttributes(context, persistence, loader, scb);
                final Properties properties = getProperties(loader);
                scb.withProperties(properties);
            }
        }
    }

   private Properties getProperties(ModelNode loader) {
      final Properties properties = new TypedProperties();
      if (loader.hasDefined(ModelKeys.PROPERTY)) {
          for (Property property : loader.get(ModelKeys.PROPERTY).asPropertyList()) {
              String propertyName = property.getName();
              Property complexValue = property.getValue().asProperty();
              String propertyValue = complexValue.getValue().asString();
              properties.setProperty(propertyName, propertyValue);
          }
      }
      return properties;
   }

   private void handleStoreProperties(OperationContext context, ModelNode cache, String storeKey, String containerName,
                                            ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException {

        if (cache.hasDefined(storeKey)) {
           for (Property storeEntry : cache.get(storeKey).asPropertyList()) {
                ModelNode store = storeEntry.getValue();

                final boolean passivation = BaseStoreResource.PASSIVATION.resolveModelAttribute(context, store).asBoolean();
                PersistenceConfigurationBuilder loadersBuilder = builder.persistence().passivation(passivation);
                StoreConfigurationBuilder<?, ?> scb = buildCacheStore(context, loadersBuilder, containerName, store, storeKey, dependencies);
                parseCommonAttributes(context, loadersBuilder, store, scb);
            }
        }
    }

   private StoreConfigurationBuilder<?, ?> buildCacheLoader(PersistenceConfigurationBuilder persistenceBuilder, ModelNode loader, String loaderKey) throws OperationFailedException {
        if (loaderKey.equals(ModelKeys.CLUSTER_LOADER)) {
            final ClusterLoaderConfigurationBuilder builder = persistenceBuilder.addClusterLoader();

            if (loader.hasDefined(ModelKeys.REMOTE_TIMEOUT)) {
                builder.remoteCallTimeout(loader.require(ModelKeys.REMOTE_TIMEOUT).asLong());
            }
            return builder;
        } else if (loaderKey.equals(ModelKeys.LOADER)) {
           String className = loader.require(ModelKeys.CLASS).asString();
           try {
              return handleStoreOrLoaderClass(className, persistenceBuilder);
           } catch (Exception e) {
              throw InfinispanMessages.MESSAGES.invalidCacheStore(e, className);
           }
        } else {
           throw new IllegalStateException();
        }

    }

    private StoreConfigurationBuilder handleStoreOrLoaderClass(String className, PersistenceConfigurationBuilder persistenceBuilder) throws ClassNotFoundException {
       if(isPresentInLoadClassLoader(className)) {
          return createStoreConfigurationFromLocalClassloader(className, persistenceBuilder);
       }
       return createDeployedStoreConfiguration(className, persistenceBuilder);
    }

   private StoreConfigurationBuilder createDeployedStoreConfiguration(String className, PersistenceConfigurationBuilder persistenceBuilder) {
      DeployedStoreConfigurationBuilder deployedStoreConfigurationBuilder = persistenceBuilder.addStore(DeployedStoreConfigurationBuilder.class);
      deployedStoreConfigurationBuilder.customStoreClassName(className);
      return deployedStoreConfigurationBuilder;
   }

   private StoreConfigurationBuilder createStoreConfigurationFromLocalClassloader(String className, PersistenceConfigurationBuilder persistenceBuilder) throws ClassNotFoundException {
      Class<?> storeImplClass = CacheLoader.class.getClassLoader().loadClass(className);
      ConfiguredBy annotation = storeImplClass.getAnnotation(ConfiguredBy.class);
      Class<? extends StoreConfigurationBuilder> builderClass = null;
      if (annotation != null) {
         Class<?> configuredBy = annotation.value();
         if (configuredBy != null) {
            BuiltBy builtBy = configuredBy.getAnnotation(BuiltBy.class);
            builderClass = builtBy.value().asSubclass(StoreConfigurationBuilder.class);
         }
      }
      if (builderClass == null) {
         return persistenceBuilder.addStore(CustomStoreConfigurationBuilder.class).customStoreClass(storeImplClass);
      }
      return persistenceBuilder.addStore(builderClass);
   }

   private boolean isPresentInLoadClassLoader(String className) {
      try {
         newInstance(className);
         return true;
      } catch (InstantiationException e) {
         throw new IllegalStateException("Could not instantiate class " + className, e);
      } catch (IllegalAccessException e) {
         throw new IllegalStateException("Class " + className + " seems not to have a default constructor", e);
      } catch (ClassNotFoundException e) {
         return false;
      }
   }

   private StoreConfigurationBuilder<?, ?> buildCacheStore(OperationContext context, PersistenceConfigurationBuilder persistenceBuilder, String containerName, ModelNode store, String storeKey, List<Dependency<?>> dependencies) throws OperationFailedException {

        ModelNode resolvedValue = null;
        if (storeKey.equals(ModelKeys.FILE_STORE)) {
            final SingleFileStoreConfigurationBuilder builder = persistenceBuilder.addStore(SingleFileStoreConfigurationBuilder.class);
            if (store.hasDefined(ModelKeys.MAX_ENTRIES)) {
                builder.maxEntries(store.get(ModelKeys.MAX_ENTRIES).asInt());
            }
            final String path = ((resolvedValue = FileStoreResource.PATH.resolveModelAttribute(context, store)).isDefined()) ? resolvedValue.asString() : InfinispanExtension.SUBSYSTEM_NAME + File.separatorChar + containerName;
            final String relativeTo = ((resolvedValue = FileStoreResource.RELATIVE_TO.resolveModelAttribute(context, store)).isDefined()) ? resolvedValue.asString() : ServerEnvironment.SERVER_DATA_DIR;
            Injector<PathManager> injector = new SimpleInjector<PathManager>() {
                volatile PathManager.Callback.Handle callbackHandle;
                @Override
                public void inject(PathManager value) {
                    callbackHandle = value.registerCallback(relativeTo, PathManager.ReloadServerCallback.create(), PathManager.Event.UPDATED, PathManager.Event.REMOVED);
                    builder.location(value.resolveRelativePathEntry(path, relativeTo));
                }

                @Override
                public void uninject() {
                    super.uninject();
                    if (callbackHandle != null) {
                        callbackHandle.remove();
                    }
                }
            };
            dependencies.add(new Dependency<PathManager>(PathManagerService.SERVICE_NAME, PathManager.class, injector));
            return builder;
        } else if (storeKey.equals(ModelKeys.STRING_KEYED_JDBC_STORE) || storeKey.equals(ModelKeys.BINARY_KEYED_JDBC_STORE) || storeKey.equals(ModelKeys.MIXED_KEYED_JDBC_STORE)) {
            ModelNode dialectNode = BaseJDBCStoreResource.DIALECT.resolveModelAttribute(context, store);
            DatabaseType databaseType = dialectNode.isDefined() ? DatabaseType.valueOf(dialectNode.asString()) : null;

            AbstractJdbcStoreConfigurationBuilder<?, ?> builder = buildJdbcStore(persistenceBuilder, context, store, databaseType);

            final String datasource = BaseJDBCStoreResource.DATA_SOURCE.resolveModelAttribute(context, store).asString();

            dependencies.add(new Dependency<Object>(ServiceName.JBOSS.append("data-source", datasource)));
            builder.dataSource().jndiUrl(datasource);
            return builder;
        } else if (storeKey.equals(ModelKeys.REMOTE_STORE)) {
            final RemoteStoreConfigurationBuilder builder = persistenceBuilder.addStore(RemoteStoreConfigurationBuilder.class);
            for (ModelNode server : store.require(ModelKeys.REMOTE_SERVERS).asList()) {
                String outboundSocketBinding = server.get(ModelKeys.OUTBOUND_SOCKET_BINDING).asString();
                Injector<OutboundSocketBinding> injector = new SimpleInjector<OutboundSocketBinding>() {
                    @Override
                    public void inject(OutboundSocketBinding value) {
                        try {
                            builder.addServer().host(value.getResolvedDestinationAddress().getHostAddress()).port(value.getDestinationPort());
                        } catch (UnknownHostException e) {
                            throw InfinispanMessages.MESSAGES.failedToInjectSocketBinding(e, value);
                        }
                    }
                };
                dependencies.add(new Dependency<OutboundSocketBinding>(OutboundSocketBinding.OUTBOUND_SOCKET_BINDING_BASE_SERVICE_NAME.append(outboundSocketBinding), OutboundSocketBinding.class, injector));
            }
            if (store.hasDefined(ModelKeys.CACHE)) {
                builder.remoteCacheName(store.get(ModelKeys.CACHE).asString());
            }
            if (store.hasDefined(ModelKeys.HOTROD_WRAPPING)) {
                builder.hotRodWrapping(store.require(ModelKeys.HOTROD_WRAPPING).asBoolean());
            }
            if (store.hasDefined(ModelKeys.RAW_VALUES)) {
                builder.rawValues(store.require(ModelKeys.RAW_VALUES).asBoolean());
            }
            if (store.hasDefined(ModelKeys.SOCKET_TIMEOUT)) {
                builder.socketTimeout(store.require(ModelKeys.SOCKET_TIMEOUT).asLong());
            }
            if (store.hasDefined(ModelKeys.TCP_NO_DELAY)) {
                builder.tcpNoDelay(store.require(ModelKeys.TCP_NO_DELAY).asBoolean());
            }
            return builder;
        } else if (storeKey.equals(ModelKeys.LEVELDB_STORE)) {
            final LevelDBStoreConfigurationBuilder builder = persistenceBuilder.addStore(LevelDBStoreConfigurationBuilder.class);
            final String path = ((resolvedValue = LevelDBStoreResource.PATH.resolveModelAttribute(context, store)).isDefined()) ? resolvedValue.asString() : InfinispanExtension.SUBSYSTEM_NAME + File.separatorChar + containerName + File.separatorChar + "data";
            final String relativeTo = ((resolvedValue = LevelDBStoreResource.RELATIVE_TO.resolveModelAttribute(context, store)).isDefined()) ? resolvedValue.asString() : ServerEnvironment.SERVER_DATA_DIR;
            Injector<PathManager> injector = new SimpleInjector<PathManager>() {
                volatile PathManager.Callback.Handle callbackHandle;
                @Override
                public void inject(PathManager value) {
                    callbackHandle = value.registerCallback(relativeTo, PathManager.ReloadServerCallback.create(), PathManager.Event.UPDATED, PathManager.Event.REMOVED);
                    builder.location(value.resolveRelativePathEntry(path, relativeTo));
                }

                @Override
                public void uninject() {
                    super.uninject();
                    if (callbackHandle != null) {
                        callbackHandle.remove();
                    }
                }
            };
            dependencies.add(new Dependency<PathManager>(PathManagerService.SERVICE_NAME, PathManager.class, injector));

            final boolean expirationDefined = store.hasDefined(ModelKeys.EXPIRATION) && store.get(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME).isDefined();
            final String expirationPath;
            if (expirationDefined) {
                ModelNode expiration = store.get(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);
                expirationPath = LevelDBExpirationResource.PATH.resolveModelAttribute(context, expiration).asString();
                builder.expiryQueueSize(LevelDBExpirationResource.QUEUE_SIZE.resolveModelAttribute(context, expiration).asInt());
            } else {
                expirationPath = InfinispanExtension.SUBSYSTEM_NAME + File.separatorChar + containerName + File.separatorChar + "expiration";
            }

            injector = new SimpleInjector<PathManager>() {
                volatile PathManager.Callback.Handle callbackHandle;
                @Override
                public void inject(PathManager value) {
                    callbackHandle = value.registerCallback(relativeTo, PathManager.ReloadServerCallback.create(), PathManager.Event.UPDATED, PathManager.Event.REMOVED);
                    builder.expiredLocation(value.resolveRelativePathEntry(expirationPath, relativeTo));
                }

                @Override
                public void uninject() {
                    super.uninject();
                    if (callbackHandle != null) {
                        callbackHandle.remove();
                    }
                }
            };
            dependencies.add(new Dependency<PathManager>(PathManagerService.SERVICE_NAME, PathManager.class, injector));

            if (store.hasDefined(ModelKeys.BLOCK_SIZE))
                builder.blockSize(store.get(ModelKeys.BLOCK_SIZE).asInt());
            if (store.hasDefined(ModelKeys.CACHE_SIZE))
                builder.cacheSize(store.get(ModelKeys.CACHE_SIZE).asLong());
            if (store.hasDefined(ModelKeys.CLEAR_THRESHOLD))
                builder.clearThreshold(store.get(ModelKeys.CLEAR_THRESHOLD).asInt());

            if (store.hasDefined(ModelKeys.COMPRESSION)) {
                ModelNode node = store.get(ModelKeys.COMPRESSION, ModelKeys.COMPRESSION_NAME);
                final CompressionType compressionType = CompressionType.valueOf(LevelDBCompressionResource.TYPE.resolveModelAttribute(context, node).asString());
                builder.compressionType(compressionType);
            }

            if (store.hasDefined(ModelKeys.IMPLEMENTATION)) {
                ModelNode node = store.get(ModelKeys.IMPLEMENTATION, ModelKeys.IMPLEMENTATION_NAME);
                final LevelDBStoreConfiguration.ImplementationType implementationType = LevelDBStoreConfiguration.ImplementationType.valueOf(LevelDBImplementationResource.TYPE.resolveModelAttribute(context, node).asString());
                builder.implementationType(implementationType);
            }
            return builder;
        } else if (storeKey.equals(ModelKeys.REST_STORE)) {
                final RestStoreConfigurationBuilder builder = persistenceBuilder.addStore(RestStoreConfigurationBuilder.class);
                builder.host("localhost"); // To pass builder validation, the builder will be configured properly when the outbound socket is ready to be injected
                for (ModelNode server : store.require(ModelKeys.REMOTE_SERVERS).asList()) {
                    String outboundSocketBinding = server.get(ModelKeys.OUTBOUND_SOCKET_BINDING).asString();
                    Injector<OutboundSocketBinding> injector = new SimpleInjector<OutboundSocketBinding>() {
                        @Override
                        public void inject(OutboundSocketBinding value) {
                            try {
                                builder.host(value.getResolvedDestinationAddress().getHostAddress()).port(value.getDestinationPort()); // FIXME: add support for multiple hosts
                            } catch (UnknownHostException e) {
                                throw InfinispanMessages.MESSAGES.failedToInjectSocketBinding(e, value);
                            }
                        }
                    };
                    dependencies.add(new Dependency<OutboundSocketBinding>(OutboundSocketBinding.OUTBOUND_SOCKET_BINDING_BASE_SERVICE_NAME.append(outboundSocketBinding), OutboundSocketBinding.class, injector));
                }
                if (store.hasDefined(ModelKeys.APPEND_CACHE_NAME_TO_PATH)) {
                    builder.appendCacheNameToPath(store.require(ModelKeys.APPEND_CACHE_NAME_TO_PATH).asBoolean());
                }
                if (store.hasDefined(ModelKeys.PATH)) {
                    builder.path(store.get(ModelKeys.PATH).asString());
                }
                builder.rawValues(true);
                builder.metadataHelper(MimeMetadataHelper.class);

                if (store.hasDefined(ModelKeys.CONNECTION_POOL)) {
                    ModelNode pool = store.get(ModelKeys.CONNECTION_POOL);
                    if (pool.hasDefined(ModelKeys.CONNECTION_TIMEOUT)) {
                        builder.connectionPool().connectionTimeout(pool.require(ModelKeys.CONNECTION_TIMEOUT).asInt());
                    }
                    if (pool.hasDefined(ModelKeys.MAX_CONNECTIONS_PER_HOST)) {
                        builder.connectionPool().maxConnectionsPerHost(pool.require(ModelKeys.MAX_CONNECTIONS_PER_HOST).asInt());
                    }
                    if (pool.hasDefined(ModelKeys.MAX_TOTAL_CONNECTIONS)) {
                        builder.connectionPool().maxTotalConnections(pool.require(ModelKeys.MAX_TOTAL_CONNECTIONS).asInt());
                    }
                    if (pool.hasDefined(ModelKeys.BUFFER_SIZE)) {
                        builder.connectionPool().bufferSize(pool.require(ModelKeys.BUFFER_SIZE).asInt());
                    }
                    if (pool.hasDefined(ModelKeys.SOCKET_TIMEOUT)) {
                        builder.connectionPool().socketTimeout(pool.require(ModelKeys.SOCKET_TIMEOUT).asInt());
                    }
                    if (pool.hasDefined(ModelKeys.TCP_NO_DELAY)) {
                        builder.connectionPool().tcpNoDelay(pool.require(ModelKeys.TCP_NO_DELAY).asBoolean());
                    }
                }
                return builder;
        } else if (storeKey.equals(ModelKeys.STORE)) {
           String className = store.require(ModelKeys.CLASS).asString();
           try {
              return handleStoreOrLoaderClass(className, persistenceBuilder);
           } catch (Exception e) {
              throw InfinispanMessages.MESSAGES.invalidCacheStore(e, className);
           }
        } else {
           throw new IllegalStateException();
        }
    }

   private Object newInstance(String className) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
      return CacheLoader.class.getClassLoader().loadClass(className).newInstance();
   }

   private void parseCommonAttributes(OperationContext context, PersistenceConfigurationBuilder persistenceBuilder, ModelNode store, StoreConfigurationBuilder storeConfigurationBuilder) throws OperationFailedException {
      ModelNode shared = store.get(ModelKeys.SHARED);
      if (shared != null && shared.isDefined()) {
         storeConfigurationBuilder.shared(shared.asBoolean());
      }
      ModelNode preload = store.get(ModelKeys.PRELOAD);
      if (preload != null && preload.isDefined()) {
         storeConfigurationBuilder.preload(preload.asBoolean());
      }
      ModelNode fetchState = store.get(ModelKeys.FETCH_STATE);
      if (fetchState != null && fetchState.isDefined()) {
         storeConfigurationBuilder.fetchPersistentState(fetchState.asBoolean());
      }
      ModelNode passivation = store.get(ModelKeys.PASSIVATION);
      if (passivation != null && passivation.isDefined()) {
         persistenceBuilder.passivation(passivation.asBoolean());
      }
      ModelNode purge = store.get(ModelKeys.PURGE);
      if (purge != null && purge.isDefined()) {
         storeConfigurationBuilder.purgeOnStartup(purge.asBoolean());
      }
      ModelNode singleton = store.get(ModelKeys.SINGLETON);
      if (singleton != null && singleton.isDefined()) {
         if (singleton.asBoolean())
            storeConfigurationBuilder.singleton().enable();
      }
      ModelNode readOnly = store.get(ModelKeys.READ_ONLY);
      if (readOnly != null && readOnly.isDefined()) {
         storeConfigurationBuilder.ignoreModifications(readOnly.asBoolean());
      }
      final boolean async = store.hasDefined(ModelKeys.WRITE_BEHIND) && store.get(ModelKeys.WRITE_BEHIND, ModelKeys.WRITE_BEHIND_NAME).isDefined();
      if (async) {
         ModelNode writeBehind = store.get(ModelKeys.WRITE_BEHIND, ModelKeys.WRITE_BEHIND_NAME);
         storeConfigurationBuilder.async().enable()
               .flushLockTimeout(StoreWriteBehindResource.FLUSH_LOCK_TIMEOUT.resolveModelAttribute(context, writeBehind).asLong())
               .modificationQueueSize(StoreWriteBehindResource.MODIFICATION_QUEUE_SIZE.resolveModelAttribute(context, writeBehind).asInt())
               .shutdownTimeout(StoreWriteBehindResource.SHUTDOWN_TIMEOUT.resolveModelAttribute(context, writeBehind).asLong())
               .threadPoolSize(StoreWriteBehindResource.THREAD_POOL_SIZE.resolveModelAttribute(context, writeBehind).asInt())
         ;
      }

      final Properties properties = new TypedProperties();
      if (store.hasDefined(ModelKeys.PROPERTY)) {
         for (Property property : store.get(ModelKeys.PROPERTY).asPropertyList()) {
            // format of properties
            // "property" => {
            //   "property-name" => {"value => "property-value"}
            // }
            String propertyName = property.getName();
            // get the value from ModelNode {"value" => "property-value"}
            ModelNode propertyValue = null ;
            propertyValue = StorePropertyResource.VALUE.resolveModelAttribute(context,property.getValue());
            properties.setProperty(propertyName, propertyValue.asString());
         }
      }
      storeConfigurationBuilder.withProperties(properties);

      ModelNode writeBehind = store.get(ModelKeys.WRITE_BEHIND);
      if (writeBehind != null && writeBehind.isDefined()) {
         if (writeBehind.asBoolean())
            storeConfigurationBuilder.async();
      }
   }

   private AbstractJdbcStoreConfigurationBuilder<?, ?> buildJdbcStore(PersistenceConfigurationBuilder loadersBuilder, OperationContext context, ModelNode store, DatabaseType databaseType) throws OperationFailedException {
        boolean useStringKeyedTable = store.hasDefined(ModelKeys.STRING_KEYED_TABLE);
        boolean useBinaryKeyedTable = store.hasDefined(ModelKeys.BINARY_KEYED_TABLE);
        if (useStringKeyedTable && !useBinaryKeyedTable) {
            JdbcStringBasedStoreConfigurationBuilder builder = loadersBuilder.addStore(JdbcStringBasedStoreConfigurationBuilder.class);
            builder.dialect(databaseType);
            this.buildStringKeyedTable(builder.table(), context, store.get(ModelKeys.STRING_KEYED_TABLE));
            return builder;
        } else if (useBinaryKeyedTable && !useStringKeyedTable) {
            JdbcBinaryStoreConfigurationBuilder builder = loadersBuilder.addStore(JdbcBinaryStoreConfigurationBuilder.class);
            builder.dialect(databaseType);
            this.buildBinaryKeyedTable(builder.table(), context, store.get(ModelKeys.BINARY_KEYED_TABLE));
            return builder;
        }
        // Else, use mixed mode
        JdbcMixedStoreConfigurationBuilder builder = loadersBuilder.addStore(JdbcMixedStoreConfigurationBuilder.class);
        builder.dialect(databaseType);
        this.buildStringKeyedTable(builder.stringTable(), context, store.get(ModelKeys.STRING_KEYED_TABLE));
        this.buildBinaryKeyedTable(builder.binaryTable(), context, store.get(ModelKeys.BINARY_KEYED_TABLE));
        return builder;
    }

    private void buildBinaryKeyedTable(TableManipulationConfigurationBuilder<?, ?> builder, OperationContext context, ModelNode table) throws OperationFailedException {
        this.buildTable(builder, context, table, "ispn_bucket");
    }

    private void buildStringKeyedTable(TableManipulationConfigurationBuilder<?, ?> builder, OperationContext context, ModelNode table) throws OperationFailedException {
        this.buildTable(builder, context, table, "ispn_entry");
    }

    private void buildTable(TableManipulationConfigurationBuilder<?, ?> builder, OperationContext context, ModelNode table, String defaultTableNamePrefix) throws OperationFailedException {
        ModelNode tableNamePrefix = BaseJDBCStoreResource.PREFIX.resolveModelAttribute(context, table);
        builder.batchSize(BaseJDBCStoreResource.BATCH_SIZE.resolveModelAttribute(context, table).asInt())
                .fetchSize(BaseJDBCStoreResource.FETCH_SIZE.resolveModelAttribute(context, table).asInt())
                .tableNamePrefix(tableNamePrefix.isDefined() ? tableNamePrefix.asString() : defaultTableNamePrefix)
                .idColumnName(this.getColumnProperty(context, table, ModelKeys.ID_COLUMN, BaseJDBCStoreResource.COLUMN_NAME, "id"))
                .idColumnType(this.getColumnProperty(context, table, ModelKeys.ID_COLUMN, BaseJDBCStoreResource.COLUMN_TYPE, "VARCHAR"))
                .dataColumnName(this.getColumnProperty(context, table, ModelKeys.DATA_COLUMN, BaseJDBCStoreResource.COLUMN_NAME, "datum"))
                .dataColumnType(this.getColumnProperty(context, table, ModelKeys.DATA_COLUMN, BaseJDBCStoreResource.COLUMN_TYPE, "BINARY"))
                .timestampColumnName(this.getColumnProperty(context, table, ModelKeys.TIMESTAMP_COLUMN, BaseJDBCStoreResource.COLUMN_NAME, "version"))
                .timestampColumnType(this.getColumnProperty(context, table, ModelKeys.TIMESTAMP_COLUMN, BaseJDBCStoreResource.COLUMN_TYPE, "BIGINT"));
    }

    private String getColumnProperty(OperationContext context, ModelNode table, String columnKey, AttributeDefinition columnAttribute, String defaultValue) throws OperationFailedException {
        if (!table.isDefined() || !table.hasDefined(columnKey)) return defaultValue;
        ModelNode column = table.get(columnKey);
        ModelNode resolvedValue = null ;
        return ((resolvedValue = columnAttribute.resolveModelAttribute(context, column)).isDefined()) ? resolvedValue.asString() : defaultValue;
    }

    public static synchronized Configuration getDefaultConfiguration(CacheMode cacheMode) {
        if (defaults == null) {
            ConfigurationBuilderHolder holder = load(DEFAULTS);
            Configuration defaultConfig = holder.getDefaultConfigurationBuilder().build();
            Map<CacheMode, Configuration> map = new EnumMap<CacheMode, Configuration>(CacheMode.class);
            map.put(defaultConfig.clustering().cacheMode(), defaultConfig);
            for (ConfigurationBuilder builder : holder.getNamedConfigurationBuilders().values()) {
                Configuration config = builder.build();
                map.put(config.clustering().cacheMode(), config);
            }
            for (CacheMode mode : CacheMode.values()) {
                if (!map.containsKey(mode)) {
                    map.put(mode, new ConfigurationBuilder().read(defaultConfig).clustering().cacheMode(mode).build());
                }
            }
            defaults = map;
        }
        return defaults.get(cacheMode);
    }

    private static ConfigurationBuilderHolder load(String resource) {
        URL url = find(resource, CacheAdd.class.getClassLoader());
        log.debugf("Loading Infinispan defaults from %s", url.toString());
        try {
            InputStream input = url.openStream();
            ParserRegistry parser = new ParserRegistry(InfinispanExtension.class.getClassLoader());
            try {
                return parser.parse(input);
            } finally {
                try {
                    input.close();
                } catch (IOException e) {
                    log.warn(e.getLocalizedMessage(), e);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Failed to parse %s", url), e);
        }
    }

    private static URL find(String resource, ClassLoader... loaders) {
        for (ClassLoader loader : loaders) {
            if (loader != null) {
                URL url = FileLookupFactory.newInstance().lookupFileLocation(resource, loader);
                if (url != null) {
                    return url;
                }
            }
        }
        throw new IllegalArgumentException(String.format("Failed to locate %s", resource));
    }

    private abstract class SimpleInjector<I> implements Injector<I> {
        @Override
        public void uninject() {
            // Do nothing
        }
    }
}
