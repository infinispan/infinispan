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

import static org.infinispan.query.impl.IndexPropertyInspector.getDataCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.getLockingCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.getMetadataCacheName;
import static org.infinispan.query.impl.IndexPropertyInspector.hasInfinispanDirectory;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.factory.CacheStoreFactory;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.jboss.as.clustering.infinispan.conflict.DeployedMergePolicyFactory;
import org.jboss.as.clustering.infinispan.conflict.DeployedMergePolicyFactoryService;
import org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactory;
import org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactoryService;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.naming.ManagedReferenceInjector;
import org.jboss.as.naming.ServiceBasedNamingStore;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.naming.service.BinderService;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.Property;
import org.jboss.logging.Logger;
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
 */
public abstract class CacheAdd extends AbstractAddStepHandler implements RestartableServiceHandler {

    private static final Logger log = Logger.getLogger(CacheAdd.class.getPackage().getName());

    final CacheMode mode;

    CacheAdd(CacheMode mode) {
        this.mode = mode;
    }

    @Override
    protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
        this.populate(operation, model);
    }

    private String getConfigurationKey() {
        switch (mode) {
            case DIST_ASYNC:
            case DIST_SYNC:
                return ModelKeys.DISTRIBUTED_CACHE_CONFIGURATION;
            case REPL_ASYNC:
            case REPL_SYNC:
                return ModelKeys.REPLICATED_CACHE_CONFIGURATION;
            case INVALIDATION_ASYNC:
            case INVALIDATION_SYNC:
                return ModelKeys.INVALIDATION_CACHE_CONFIGURATION;
            case LOCAL:
                return ModelKeys.LOCAL_CACHE_CONFIGURATION;
            default:
                throw new IllegalArgumentException("Unknown cache mode " + mode);
        }
    }

    /**
     * Extract indexing information for the cache from the model.
     *
     * @return a {@link Properties} with the indexing properties or empty if the cache is not indexed
     */
    private Properties extractIndexingProperties(OperationContext context, ModelNode operation, String cacheConfiguration) {
        Properties properties = new Properties();
        PathAddress cacheAddress = getCacheAddressFromOperation(operation);
        Resource cacheConfigResource = context.readResourceFromRoot(cacheAddress.subAddress(0, 2), true)
              .getChild(PathElement.pathElement(ModelKeys.CONFIGURATIONS, ModelKeys.CONFIGURATIONS_NAME))
              .getChild(PathElement.pathElement(getConfigurationKey(), cacheConfiguration));

        PathElement indexingPathElement = PathElement.pathElement(ModelKeys.INDEXING, ModelKeys.INDEXING_NAME);

        if (cacheConfigResource == null || !cacheConfigResource.hasChild(indexingPathElement)) {
            return properties;
        }

        Resource indexingResource = cacheConfigResource.getChild(indexingPathElement);
        if (indexingResource == null) return properties;

        ModelNode indexingModel = indexingResource.getModel();
        boolean hasProperties = indexingModel.hasDefined(ModelKeys.INDEXING_PROPERTIES);

        if (!hasProperties) return properties;

        ModelNode modelNode = indexingResource.getModel().get(ModelKeys.INDEXING_PROPERTIES);
        List<Property> modelProperties = modelNode.asPropertyList();
        modelProperties.forEach(p -> properties.put(p.getName(), p.getValue().asString()));
        return properties;
    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
        // Because we use child resources in a read-only manner to configure the cache, replace the local model with the full model
        ModelNode cacheModel = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));

        // we also need the containerModel
        PathAddress containerAddress = getCacheContainerAddressFromOperation(operation);
        ModelNode containerModel = context.readResourceFromRoot(containerAddress).getModel();

        // install the services from a reusable method
        this.installRuntimeServices(context, operation, containerModel, cacheModel);
    }

    @Override
    public Collection<ServiceController<?>> installRuntimeServices(OperationContext context, ModelNode operation, ModelNode containerModel, ModelNode cacheModel) throws OperationFailedException {

        // get all required addresses, names and service names
        PathAddress cacheAddress = getCacheAddressFromOperation(operation);
        PathAddress containerAddress = getCacheContainerAddressFromOperation(operation);
        String cacheName = cacheAddress.getLastElement().getValue();
        String containerName = containerAddress.getLastElement().getValue();

        // get model attributes
        final String configuration = CacheResource.CONFIGURATION.resolveModelAttribute(context, cacheModel).asString();
        Properties indexingProperties = extractIndexingProperties(context, operation, configuration);
        ServiceTarget target = context.getServiceTarget();

        Collection<ServiceController<?>> controllers = new ArrayList<>(2);
        // now install the corresponding cache service (starts a configured cache)
        controllers.add(this.installCacheService(target, containerName, cacheName, configuration, indexingProperties));

        // install a name service entry for the cache
        ModelNode resolvedValue = CacheConfigurationResource.JNDI_NAME.resolveModelAttribute(context, cacheModel);
        final String jndiName = InfinispanJndiName.createCacheJndiName(resolvedValue.isDefined() ? resolvedValue.asString() : null, containerName, cacheName);
        controllers.add(this.installJndiService(target, containerName, cacheName, jndiName));
        log.debugf("Cache service for cache %s installed for container %s", cacheName, containerName);

        return controllers;
    }

    @Override
    public void removeRuntimeServices(OperationContext context, ModelNode operation, ModelNode containerModel, ModelNode cacheModel)
            throws OperationFailedException {
        // get container and cache addresses
        final PathAddress cacheAddress = getCacheAddressFromOperation(operation) ;
        final PathAddress containerAddress = getCacheContainerAddressFromOperation(operation) ;
        // get container and cache names
        final String cacheName = cacheAddress.getLastElement().getValue() ;
        final String containerName = containerAddress.getLastElement().getValue() ;

        // remove the binder service
        ModelNode resolvedValue = CacheConfigurationResource.JNDI_NAME.resolveModelAttribute(context, cacheModel);
        final String jndiName = InfinispanJndiName.createCacheJndiName(resolvedValue.isDefined() ? resolvedValue.asString() : null, containerName, cacheName);
        context.removeService(ContextNames.bindInfoFor(jndiName).getBinderServiceName());

        // remove the CacheService instance
        context.removeService(CacheServiceName.CACHE.getServiceName(containerName, cacheName));

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

    ServiceController<?> installCacheService(ServiceTarget target, String containerName, String cacheName,
                                             String configurationName, Properties indexingProperties) {

        final InjectedValue<EmbeddedCacheManager> container = new InjectedValue<>();
        final CacheDependencies cacheDependencies = new CacheDependencies(container);
        final Service<Cache<Object, Object>> service = new CacheService<>(cacheName, configurationName, cacheDependencies);
        final ServiceBuilder<?> builder = target.addService(CacheServiceName.CACHE.getServiceName(containerName, cacheName), service)
                .addDependency(CacheServiceName.CONFIGURATION.getServiceName(containerName, configurationName))
                .addDependency(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(containerName), EmbeddedCacheManager.class, container)
                .setInitialMode(ServiceController.Mode.ACTIVE);

        builder.addDependency(DeployedCacheStoreFactoryService.SERVICE_NAME, DeployedCacheStoreFactory.class, cacheDependencies.getDeployedCacheStoreFactoryInjector());
        builder.addDependency(DeployedMergePolicyFactoryService.SERVICE_NAME, DeployedMergePolicyFactory.class, cacheDependencies.getDeployedMergePolicyRegistryInjector());

        boolean hasInfinispanDirectory = hasInfinispanDirectory(indexingProperties);

        if (hasInfinispanDirectory) {
            builder.addDependency(CacheServiceName.CACHE.getServiceName(containerName, getDataCacheName(indexingProperties)));
            builder.addDependency(CacheServiceName.CACHE.getServiceName(containerName, getMetadataCacheName(indexingProperties)));
            builder.addDependency(CacheServiceName.CACHE.getServiceName(containerName, getLockingCacheName(indexingProperties)));
        }

        return builder.install();
    }

    @SuppressWarnings("rawtypes")
    ServiceController<?> installJndiService(ServiceTarget target, String containerName, String cacheName, String jndiName) {

        final ServiceName cacheServiceName = CacheServiceName.CACHE.getServiceName(containerName, cacheName);
        final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(jndiName);
        final BinderService binder = new BinderService(bindInfo.getBindName());
        return target.addService(bindInfo.getBinderServiceName(), binder)
                .addAliases(ContextNames.JAVA_CONTEXT_SERVICE_NAME.append(jndiName))
                .addDependency(cacheServiceName, Cache.class, new ManagedReferenceInjector<>(binder.getManagedObjectInjector()))
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
        for(AttributeDefinition attr : CacheResource.CACHE_ATTRIBUTES) {
            attr.validateAndSet(fromModel, toModel);
        }
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

    private static class CacheDependencies implements CacheService.Dependencies {

        private final Value<EmbeddedCacheManager> container;
        private final InjectedValue<XAResourceRecoveryRegistry> recoveryRegistry = new InjectedValue<>();
        private final InjectedValue<DeployedCacheStoreFactory> deployedCacheStoreFactory = new InjectedValue<>();
        private final InjectedValue<DeployedMergePolicyFactory> deployedMergePolicyRegistry = new InjectedValue<>();

        CacheDependencies(Value<EmbeddedCacheManager> container) {
            this.container = container;
        }

        Injector<XAResourceRecoveryRegistry> getRecoveryRegistryInjector() {
            return this.recoveryRegistry;
        }

        public InjectedValue<DeployedCacheStoreFactory> getDeployedCacheStoreFactoryInjector() {
           return deployedCacheStoreFactory;
        }

        public InjectedValue<DeployedMergePolicyFactory> getDeployedMergePolicyRegistryInjector() {
            return deployedMergePolicyRegistry;
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

        @Override
        public DeployedMergePolicyFactory getDeployedMergePolicyRegistry() {
            return deployedMergePolicyRegistry.getValue();
        }
    }
}
