/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.compatibility.adaptor52x.Adaptor52xStoreConfigurationBuilder;
import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.configuration.cache.SitesConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
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
import org.infinispan.transaction.tm.BatchModeTransactionManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.jboss.as.clustering.infinispan.InfinispanMessages;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.controller.services.path.PathManagerService;
import org.jboss.as.naming.ManagedReferenceInjector;
import org.jboss.as.naming.ServiceBasedNamingStore;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.naming.service.BinderService;
import org.jboss.as.network.OutboundSocketBinding;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.as.server.Services;
import org.jboss.as.txn.service.TxnServices;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.Property;
import org.jboss.logging.Logger;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoader;
import org.jboss.msc.inject.Injector;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.value.InjectedValue;
import org.jboss.msc.value.Value;
import org.jboss.tm.XAResourceRecoveryRegistry;

import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * Base class for cache add handlers
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public abstract class CacheAdd extends AbstractAddStepHandler {

    private static final Logger log = Logger.getLogger(CacheAdd.class.getPackage().getName());
    private static final String DEFAULTS = "infinispan-defaults.xml";
    private static volatile Map<CacheMode, Configuration> defaults = null;

    private static final String[] loaderKeys = new String[] { ModelKeys.LOADER, ModelKeys.CLUSTER_LOADER };
    private static final String[] storeKeys = new String[] { ModelKeys.STORE, ModelKeys.FILE_STORE,
            ModelKeys.STRING_KEYED_JDBC_STORE, ModelKeys.BINARY_KEYED_JDBC_STORE, ModelKeys.MIXED_KEYED_JDBC_STORE,
            ModelKeys.REMOTE_STORE, ModelKeys.LEVELDB_STORE, ModelKeys.REST_STORE };

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
                URL url = loader.getResource(resource);
                if (url != null) {
                    return url;
                }
            }
        }
        throw new IllegalArgumentException(String.format("Failed to locate %s", resource));
    }

    final CacheMode mode;

    CacheAdd(CacheMode mode) {
        this.mode = mode;
    }

    @Override
    protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {

        this.populate(operation, model);
    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model, ServiceVerificationHandler verificationHandler, List<ServiceController<?>> newControllers) throws OperationFailedException {

        // Because we use child resources in a read-only manner to configure the cache, replace the local model with the full model
        ModelNode cacheModel = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));

        // we also need the containerModel
        PathAddress containerAddress = getCacheContainerAddressFromOperation(operation);
        ModelNode containerModel = context.readResourceFromRoot(containerAddress).getModel();

        // install the services from a reusable method
        newControllers.addAll(this.installRuntimeServices(context, operation, containerModel, cacheModel, verificationHandler));
    }

    Collection<ServiceController<?>> installRuntimeServices(OperationContext context, ModelNode operation, ModelNode containerModel, ModelNode cacheModel, ServiceVerificationHandler verificationHandler) throws OperationFailedException {

        // get all required addresses, names and service names
        PathAddress cacheAddress = getCacheAddressFromOperation(operation);
        PathAddress containerAddress = getCacheContainerAddressFromOperation(operation);
        String cacheName = cacheAddress.getLastElement().getValue();
        String containerName = containerAddress.getLastElement().getValue();

        // get model attributes
        ModelNode resolvedValue = null;
        final String jndiName = ((resolvedValue = CacheResource.JNDI_NAME.resolveModelAttribute(context, cacheModel)).isDefined()) ? resolvedValue.asString() : null;
        final ServiceController.Mode initialMode = StartMode.valueOf(CacheResource.START.resolveModelAttribute(context, cacheModel).asString()).getMode();

        final ModuleIdentifier moduleId = (resolvedValue = CacheResource.CACHE_MODULE.resolveModelAttribute(context, cacheModel)).isDefined() ? ModuleIdentifier.fromString(resolvedValue.asString()) : null;

        // create a list for dependencies which may need to be added during processing
        List<Dependency<?>> dependencies = new LinkedList<Dependency<?>>();
        // Infinispan Configuration to hold the operation data
        ConfigurationBuilder builder = new ConfigurationBuilder().read(getDefaultConfiguration(this.mode));

        // process cache configuration ModelNode describing overrides to defaults
        processModelNode(context, containerName, cacheModel, builder, dependencies);

        // get container Model to pick up the value of the default cache of the container
        // AS7-3488 make default-cache no required attribute
        String defaultCache = CacheContainerResource.DEFAULT_CACHE.resolveModelAttribute(context, containerModel).asString();

        ServiceTarget target = context.getServiceTarget();
        Configuration config = builder.build();

        Collection<ServiceController<?>> controllers = new ArrayList<ServiceController<?>>(3);

        // install the cache configuration service (configures a cache)
        controllers.add(this.installCacheConfigurationService(target, containerName, cacheName, defaultCache, moduleId,
                        builder, config, dependencies, verificationHandler));
        log.debugf("Cache configuration service for %s installed for container %s", cacheName, containerName);

        // now install the corresponding cache service (starts a configured cache)
        controllers.add(this.installCacheService(target, containerName, cacheName, defaultCache, initialMode, config, verificationHandler));

        // install a name service entry for the cache
        controllers.add(this.installJndiService(target, containerName, cacheName, InfinispanJndiName.createCacheJndiName(jndiName, containerName, cacheName), verificationHandler));
        log.debugf("Cache service for cache %s installed for container %s", cacheName, containerName);

        return controllers;
    }

    void removeRuntimeServices(OperationContext context, ModelNode operation, ModelNode model)
            throws OperationFailedException {
        // get container and cache addresses
        final PathAddress cacheAddress = getCacheAddressFromOperation(operation) ;
        final PathAddress containerAddress = getCacheContainerAddressFromOperation(operation) ;
        // get container and cache names
        final String cacheName = cacheAddress.getLastElement().getValue() ;
        final String containerName = containerAddress.getLastElement().getValue() ;

        // remove all services started by CacheAdd, in reverse order
        // remove the binder service
        ModelNode resolvedValue = null;
        final String jndiName = (resolvedValue = CacheResource.JNDI_NAME.resolveModelAttribute(context, model)).isDefined() ? resolvedValue.asString() : null;
        ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(InfinispanJndiName.createCacheJndiName(jndiName, containerName, cacheName));
        context.removeService(bindInfo.getBinderServiceName()) ;
        // remove the CacheService instance
        context.removeService(CacheService.getServiceName(containerName, cacheName));
        // remove the cache configuration service
        context.removeService(CacheConfigurationService.getServiceName(containerName, cacheName));

        log.debugf("cache %s removed for container %s", cacheName, containerName);
    }

    protected PathAddress getCacheAddressFromOperation(ModelNode operation) {
        return PathAddress.pathAddress(operation.get(OP_ADDR)) ;
    }

    protected PathAddress getCacheContainerAddressFromOperation(ModelNode operation) {
        final PathAddress cacheAddress = getCacheAddressFromOperation(operation) ;
        final PathAddress containerAddress = cacheAddress.subAddress(0, cacheAddress.size()-1) ;
        return containerAddress ;
    }

    ServiceController<?> installCacheConfigurationService(ServiceTarget target, String containerName, String cacheName, String defaultCache, ModuleIdentifier moduleId,
            ConfigurationBuilder builder, Configuration config, List<Dependency<?>> dependencies, ServiceVerificationHandler verificationHandler) {

        final InjectedValue<EmbeddedCacheManager> container = new InjectedValue<EmbeddedCacheManager>();
        final CacheConfigurationDependencies cacheConfigurationDependencies = new CacheConfigurationDependencies(container);
        final Service<Configuration> service = new CacheConfigurationService(cacheName, builder, moduleId, cacheConfigurationDependencies);
        final ServiceBuilder<?> configBuilder = target.addService(CacheConfigurationService.getServiceName(containerName, cacheName), service)
                .addDependency(EmbeddedCacheManagerService.getServiceName(containerName), EmbeddedCacheManager.class, container)
                .addDependency(Services.JBOSS_SERVICE_MODULE_LOADER, ModuleLoader.class, cacheConfigurationDependencies.getModuleLoaderInjector())
                .setInitialMode(ServiceController.Mode.PASSIVE)
        ;
        if (config.invocationBatching().enabled()) {
            cacheConfigurationDependencies.getTransactionManagerInjector().inject(BatchModeTransactionManager.getInstance());
        } else if (config.transaction().transactionMode() == org.infinispan.transaction.TransactionMode.TRANSACTIONAL) {
            configBuilder.addDependency(TxnServices.JBOSS_TXN_TRANSACTION_MANAGER, TransactionManager.class, cacheConfigurationDependencies.getTransactionManagerInjector());
            if (config.transaction().useSynchronization()) {
                configBuilder.addDependency(TxnServices.JBOSS_TXN_SYNCHRONIZATION_REGISTRY, TransactionSynchronizationRegistry.class, cacheConfigurationDependencies.getTransactionSynchronizationRegistryInjector());
            }
        }

        // add in any additional dependencies resulting from ModelNode parsing
        for (Dependency<?> dependency : dependencies) {
            this.addDependency(configBuilder, dependency);
        }
        // add an alias for the default cache
        if (cacheName.equals(defaultCache)) {
            configBuilder.addAliases(CacheConfigurationService.getServiceName(containerName, null));
        }
        return configBuilder.install();
    }

    ServiceController<?> installCacheService(ServiceTarget target, String containerName, String cacheName, String defaultCache, ServiceController.Mode initialMode,
            Configuration config, ServiceVerificationHandler verificationHandler) {

        final InjectedValue<EmbeddedCacheManager> container = new InjectedValue<EmbeddedCacheManager>();
        final CacheDependencies cacheDependencies = new CacheDependencies(container);
        final Service<Cache<Object, Object>> service = new CacheService<Object, Object>(cacheName, cacheDependencies);
        final ServiceBuilder<?> builder = target.addService(CacheService.getServiceName(containerName, cacheName), service)
                .addDependency(CacheConfigurationService.getServiceName(containerName, cacheName))
                .addDependency(EmbeddedCacheManagerService.getServiceName(containerName), EmbeddedCacheManager.class, container)
                .setInitialMode(initialMode)
        ;
        if (config.transaction().recovery().enabled()) {
            builder.addDependency(TxnServices.JBOSS_TXN_ARJUNA_RECOVERY_MANAGER, XAResourceRecoveryRegistry.class, cacheDependencies.getRecoveryRegistryInjector());
        }

        // add an alias for the default cache
        if (cacheName.equals(defaultCache)) {
            builder.addAliases(CacheService.getServiceName(containerName, null));
        }

        if (initialMode == ServiceController.Mode.ACTIVE) {
            builder.addListener(verificationHandler);
        }

        return builder.install();
    }

    @SuppressWarnings("rawtypes")
    ServiceController<?> installJndiService(ServiceTarget target, String containerName, String cacheName, String jndiName, ServiceVerificationHandler verificationHandler) {

        final ServiceName cacheServiceName = CacheService.getServiceName(containerName, cacheName);
        final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(jndiName);
        final BinderService binder = new BinderService(bindInfo.getBindName());
        return target.addService(bindInfo.getBinderServiceName(), binder)
                .addAliases(ContextNames.JAVA_CONTEXT_SERVICE_NAME.append(jndiName))
                .addDependency(cacheServiceName, Cache.class, new ManagedReferenceInjector<Cache>(binder.getManagedObjectInjector()))
                .addDependency(bindInfo.getParentContextServiceName(), ServiceBasedNamingStore.class, binder.getNamingStoreInjector())
                .setInitialMode(ServiceController.Mode.PASSIVE)
                .install()
        ;
    }

    private <T> void addDependency(ServiceBuilder<?> builder, Dependency<T> dependency) {
        final ServiceName name = dependency.getName();
        final Injector<T> injector = dependency.getInjector();
        if (injector != null) {
            builder.addDependency(name, dependency.getType(), injector);
        } else {
            builder.addDependency(name);
        }
    }

    /**
     * Transfer elements common to both operations and models
     *
     * @param fromModel
     * @param toModel
     */
    void populate(ModelNode fromModel, ModelNode toModel) throws OperationFailedException {

        CacheResource.START.validateAndSet(fromModel, toModel);
        CacheResource.BATCHING.validateAndSet(fromModel, toModel);
        CacheResource.INDEXING.validateAndSet(fromModel, toModel);
        CacheResource.JNDI_NAME.validateAndSet(fromModel, toModel);
        CacheResource.CACHE_MODULE.validateAndSet(fromModel, toModel);
        CacheResource.INDEXING_PROPERTIES.validateAndSet(fromModel, toModel);
        CacheResource.STATISTICS.validateAndSet(fromModel, toModel);
    }

    /**
     * Create a Configuration object initialized from the operation ModelNode
     *
     * @param containerName the name of the cache container
     * @param cache         ModelNode representing cache configuration
     * @param builder       ConfigurationBuilder object to add data to
     * @return initialised Configuration object
     */
    void processModelNode(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException {

        builder.jmxStatistics().enabled(CacheResource.STATISTICS.resolveModelAttribute(context, cache).asBoolean());

        final Indexing indexing = Indexing.valueOf(CacheResource.INDEXING.resolveModelAttribute(context, cache).asString());
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
                .enabled(indexing.isEnabled())
                .indexLocalOnly(indexing.isLocalOnly())
                .withProperties(indexingProperties)
        ;

        // locking is a child resource
        if (cache.hasDefined(ModelKeys.LOCKING) && cache.get(ModelKeys.LOCKING, ModelKeys.LOCKING_NAME).isDefined()) {
            ModelNode locking = cache.get(ModelKeys.LOCKING, ModelKeys.LOCKING_NAME);

            final IsolationLevel isolationLevel = IsolationLevel.valueOf(LockingResource.ISOLATION.resolveModelAttribute(context, locking).asString());
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
                final int maxEntries = EvictionResource.MAX_ENTRIES.resolveModelAttribute(context, eviction).asInt();
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
                sitesBuilder
                        .addBackup()
                        .site(siteName)
                        .backupFailurePolicy(BackupFailurePolicy.valueOf(BackupSiteResource.FAILURE_POLICY.resolveModelAttribute(context, site).asString()))
                        .strategy(BackupStrategy.valueOf(BackupSiteResource.STRATEGY.resolveModelAttribute(context, site).asString()))
                        .replicationTimeout(BackupSiteResource.REPLICATION_TIMEOUT.resolveModelAttribute(context, site).asLong());
                if (BackupSiteResource.ENABLED.resolveModelAttribute(context, site).asBoolean()) {
                    sitesBuilder.addInUseBackupSite(siteName);
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
              org.infinispan.loaders.CacheLoader loaderInstance = (org.infinispan.loaders.CacheLoader) newInstance(className);
              Adaptor52xStoreConfigurationBuilder scb = persistenceBuilder.addStore(Adaptor52xStoreConfigurationBuilder.class);
              scb.loader(loaderInstance);
              return scb;
           } catch (Exception e) {
              throw InfinispanMessages.MESSAGES.invalidCacheLoader(e, className);
           }
        } else {
           throw new IllegalStateException();
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
            final AbstractJdbcStoreConfigurationBuilder<?, ?> builder = this.buildJdbcStore(persistenceBuilder, context, store);

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
                            builder.addServer().host(value.getDestinationAddress().getHostAddress()).port(value.getDestinationPort());
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
            if (store.hasDefined(ModelKeys.QUEUE_SIZE))
                builder.expiryQueueSize(store.get(ModelKeys.QUEUE_SIZE).asInt());

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
                                builder.host(value.getDestinationAddress().getHostAddress()).port(value.getDestinationPort()); // FIXME: add support for multiple hosts
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
              Object instance = newInstance(className);
              StoreConfigurationBuilder storeConfigurationBuilder;
              if (instance instanceof org.infinispan.loaders.CacheLoader) { //5.2.x configuration
                 Adaptor52xStoreConfigurationBuilder scb = persistenceBuilder.addStore(Adaptor52xStoreConfigurationBuilder.class);
                 scb.loader((org.infinispan.loaders.CacheLoader) instance);
                 storeConfigurationBuilder = scb;
              } else {
                 Class<?> storeImplClass = CacheLoader.class.getClassLoader().loadClass(className);
                 Class<? extends StoreConfigurationBuilder> builderClass = StoreConfigurationBuilder.class.getClassLoader().loadClass(storeImplClass.getName() + "ConfigurationBuilder").asSubclass(StoreConfigurationBuilder.class);
                 storeConfigurationBuilder = persistenceBuilder.addStore(builderClass);
              }
              return storeConfigurationBuilder;
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

   private AbstractJdbcStoreConfigurationBuilder<?, ?> buildJdbcStore(PersistenceConfigurationBuilder loadersBuilder, OperationContext context, ModelNode store) throws OperationFailedException {
        boolean useStringKeyedTable = store.hasDefined(ModelKeys.STRING_KEYED_TABLE);
        boolean useBinaryKeyedTable = store.hasDefined(ModelKeys.BINARY_KEYED_TABLE);
        if (useStringKeyedTable && !useBinaryKeyedTable) {
            JdbcStringBasedStoreConfigurationBuilder builder = loadersBuilder.addStore(JdbcStringBasedStoreConfigurationBuilder.class);
            this.buildStringKeyedTable(builder.table(), context, store.get(ModelKeys.STRING_KEYED_TABLE));
            return builder;
        } else if (useBinaryKeyedTable && !useStringKeyedTable) {
            JdbcBinaryStoreConfigurationBuilder builder = loadersBuilder.addStore(JdbcBinaryStoreConfigurationBuilder.class);
            this.buildBinaryKeyedTable(builder.table(), context, store.get(ModelKeys.BINARY_KEYED_TABLE));
            return builder;
        }
        // Else, use mixed mode
        JdbcMixedStoreConfigurationBuilder builder = loadersBuilder.addStore(JdbcMixedStoreConfigurationBuilder.class);
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

    private String getColumnProperty(OperationContext context, ModelNode table, String columnKey, AttributeDefinition columnAttribute, String defaultValue) throws OperationFailedException
    {
        if (!table.isDefined() || !table.hasDefined(columnKey)) return defaultValue;
        ModelNode column = table.get(columnKey);
        ModelNode resolvedValue = null ;
        return ((resolvedValue = columnAttribute.resolveModelAttribute(context, column)).isDefined()) ? resolvedValue.asString() : defaultValue;
    }

    /*
     * Allows us to store dependency requirements for later processing.
     */
    protected class Dependency<I> {
        private final ServiceName name;
        private final Class<I> type;
        private final Injector<I> target;

        Dependency(ServiceName name) {
            this(name, null, null);
        }

        Dependency(ServiceName name, Class<I> type, Injector<I> target) {
            this.name = name;
            this.type = type;
            this.target = target;
        }

        ServiceName getName() {
            return name;
        }

        public Class<I> getType() {
            return type;
        }

        public Injector<I> getInjector() {
            return target;
        }
    }

    private abstract class SimpleInjector<I> implements Injector<I> {
        @Override
        public void uninject() {
            // Do nothing
        }
    }

    private static class CacheDependencies implements CacheService.Dependencies {

        private final Value<EmbeddedCacheManager> container;
        private final InjectedValue<XAResourceRecoveryRegistry> recoveryRegistry = new InjectedValue<XAResourceRecoveryRegistry>();

        CacheDependencies(Value<EmbeddedCacheManager> container) {
            this.container = container;
        }

        Injector<XAResourceRecoveryRegistry> getRecoveryRegistryInjector() {
            return this.recoveryRegistry;
        }

        @Override
        public EmbeddedCacheManager getCacheContainer() {
            return this.container.getValue();
        }

        @Override
        public XAResourceRecoveryRegistry getRecoveryRegistry() {
            return this.recoveryRegistry.getOptionalValue();
        }
    }

    private static class CacheConfigurationDependencies implements CacheConfigurationService.Dependencies {

        private final Value<EmbeddedCacheManager> container;
        private final InjectedValue<TransactionManager> tm = new InjectedValue<TransactionManager>();
        private final InjectedValue<TransactionSynchronizationRegistry> tsr = new InjectedValue<TransactionSynchronizationRegistry>();
        private final InjectedValue<ModuleLoader> moduleLoader = new InjectedValue<ModuleLoader>();

        CacheConfigurationDependencies(Value<EmbeddedCacheManager> container) {
            this.container = container;
        }

        Injector<TransactionManager> getTransactionManagerInjector() {
            return this.tm;
        }

        Injector<TransactionSynchronizationRegistry> getTransactionSynchronizationRegistryInjector() {
            return this.tsr;
        }

        Injector<ModuleLoader> getModuleLoaderInjector() {
            return this.moduleLoader;
        }

        @Override
        public EmbeddedCacheManager getCacheContainer() {
            return this.container.getValue();
        }

        @Override
        public TransactionManager getTransactionManager() {
            return this.tm.getOptionalValue();
        }

        @Override
        public TransactionSynchronizationRegistry getTransactionSynchronizationRegistry() {
            return this.tsr.getOptionalValue();
        }

        @Override
        public ModuleLoader getModuleLoader() {
            return this.moduleLoader.getValue();
        }
    }
}
