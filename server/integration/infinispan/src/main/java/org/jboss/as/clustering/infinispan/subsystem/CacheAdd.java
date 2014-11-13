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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.factory.CacheStoreFactory;
import org.infinispan.transaction.tm.BatchModeTransactionManager;
import org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactory;
import org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactoryService;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.naming.ManagedReferenceInjector;
import org.jboss.as.naming.ServiceBasedNamingStore;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.naming.service.BinderService;
import org.jboss.as.server.Services;
import org.jboss.as.txn.service.TxnServices;
import org.jboss.dmr.ModelNode;
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

/**
 * Base class for cache add handlers
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Tristan Tarrant
 */
public abstract class CacheAdd extends AbstractAddStepHandler {
    private static final Logger log = Logger.getLogger(CacheAdd.class.getPackage().getName());

    final CacheMode mode;
    final boolean template;

    CacheAdd(CacheMode mode, boolean template) {
        this.mode = mode;
        this.template = template;
    }

    CacheAdd(CacheMode mode) {
        this(mode, false);
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
        StartMode startMode = StartMode.valueOf(CacheResource.START.resolveModelAttribute(context, cacheModel).asString());
        if (startMode != StartMode.EAGER) {
           log.warnf("Ignoring start mode [%s] of cache service [%s], as EAGER is the only supported mode", startMode, cacheName);
           startMode = StartMode.EAGER;
        }
        final ServiceController.Mode initialMode = startMode.getMode();

        final ModuleIdentifier moduleId = (resolvedValue = CacheResource.CACHE_MODULE.resolveModelAttribute(context, cacheModel)).isDefined() ? ModuleIdentifier.fromString(resolvedValue.asString()) : null;

        // create a list for dependencies which may need to be added during processing
        List<Dependency<?>> dependencies = new LinkedList<Dependency<?>>();

        // Infinispan Configuration to hold the operation data
        ModelConfigurationBuilder modelBuilder = new ModelConfigurationBuilder(this.mode);
        ConfigurationBuilder builder = modelBuilder.processModelNode(context, containerName, cacheModel, dependencies);

        // get container Model to pick up the value of the default cache of the container
        // AS7-3488 make default-cache no required attribute
        String defaultCache = CacheContainerResource.DEFAULT_CACHE.resolveModelAttribute(context, containerModel).asString();

        ServiceTarget target = context.getServiceTarget();
        Configuration config = builder.build();

        Collection<ServiceController<?>> controllers = new ArrayList<ServiceController<?>>(3);

        // install the cache configuration service (configures a cache)
        controllers.add(this.installCacheConfigurationService(target, containerName, cacheName, defaultCache, moduleId,
                        builder, config, dependencies, verificationHandler, modelBuilder.getCacheConfigurationDependencies()));
        log.debugf("Cache configuration service for %s installed for container %s", cacheName, containerName);

        if (!template) {
            // now install the corresponding cache service (starts a configured cache)
            controllers.add(this.installCacheService(target, containerName, cacheName, defaultCache, initialMode, config, verificationHandler));

            // install a name service entry for the cache
            controllers.add(this.installJndiService(target, containerName, cacheName, InfinispanJndiName.createCacheJndiName(jndiName, containerName, cacheName), verificationHandler));
            log.debugf("Cache service for cache %s installed for container %s", cacheName, containerName);
        }
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
        if (!template) {
            final String jndiName = (resolvedValue = CacheResource.JNDI_NAME.resolveModelAttribute(context, model)).isDefined() ? resolvedValue.asString() : null;
            ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(InfinispanJndiName.createCacheJndiName(jndiName, containerName, cacheName));
            context.removeService(bindInfo.getBinderServiceName()) ;
            // remove the CacheService instance
            context.removeService(CacheService.getServiceName(containerName, cacheName));
        }
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
            ConfigurationBuilder builder, Configuration config, List<Dependency<?>> dependencies, ServiceVerificationHandler verificationHandler, CacheConfigurationDependencies cacheConfigurationDependencies) {
        final Service<Configuration> service = new CacheConfigurationService(cacheName, builder, moduleId, cacheConfigurationDependencies);
        final ServiceBuilder<?> configBuilder = target.addService(CacheConfigurationService.getServiceName(containerName, cacheName), service)
                .addDependency(EmbeddedCacheManagerService.getServiceName(containerName), EmbeddedCacheManager.class, cacheConfigurationDependencies.getContainerInjector())
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

        builder.addDependency(DeployedCacheStoreFactoryService.SERVICE_NAME, DeployedCacheStoreFactory.class, cacheDependencies.getDeployedCacheStoreFactoryInjector());

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
        CacheResource.CONFIGURATION.validateAndSet(fromModel, toModel);
        CacheResource.BATCHING.validateAndSet(fromModel, toModel);
        CacheResource.INDEXING.validateAndSet(fromModel, toModel);
        CacheResource.INDEXING_AUTO_CONFIG.validateAndSet(fromModel, toModel);
        CacheResource.JNDI_NAME.validateAndSet(fromModel, toModel);
        CacheResource.CACHE_MODULE.validateAndSet(fromModel, toModel);
        CacheResource.INDEXING_PROPERTIES.validateAndSet(fromModel, toModel);
        CacheResource.STATISTICS.validateAndSet(fromModel, toModel);
        CacheResource.REMOTE_CACHE.validateAndSet(fromModel, toModel);
        CacheResource.REMOTE_SITE.validateAndSet(fromModel, toModel);
    }

    /*
     * Allows us to store dependency requirements for later processing.
     */
    protected static class Dependency<I> {
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

    private static class CacheDependencies implements CacheService.Dependencies {

        private final Value<EmbeddedCacheManager> container;
        private final InjectedValue<XAResourceRecoveryRegistry> recoveryRegistry = new InjectedValue<XAResourceRecoveryRegistry>();
        private final InjectedValue<DeployedCacheStoreFactory> deployedCacheStoreFactory = new InjectedValue<DeployedCacheStoreFactory>();

        CacheDependencies(Value<EmbeddedCacheManager> container) {
            this.container = container;
        }

        Injector<XAResourceRecoveryRegistry> getRecoveryRegistryInjector() {
            return this.recoveryRegistry;
        }

        public InjectedValue<DeployedCacheStoreFactory> getDeployedCacheStoreFactoryInjector() {
           return deployedCacheStoreFactory;
        }

        @Override
        public EmbeddedCacheManager getCacheContainer() {
            return this.container.getValue();
        }

        @Override
        public XAResourceRecoveryRegistry getRecoveryRegistry() {
            return this.recoveryRegistry.getOptionalValue();
        }

        @Override
        public CacheStoreFactory getDeployedCacheStoreFactory() {
           return deployedCacheStoreFactory.getValue();
        }
    }
}
