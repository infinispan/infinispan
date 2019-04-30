/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.counter.configuration.Reliability;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.server.commons.dmr.ModelNodes;
import org.infinispan.server.commons.naming.BinderServiceBuilder;
import org.infinispan.server.commons.naming.JndiNameFactory;
import org.infinispan.server.commons.service.AliasServiceBuilder;
import org.infinispan.server.infinispan.spi.CacheContainer;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceNameFactory;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.infinispan.server.infinispan.spi.service.CacheServiceNameFactory;
import org.infinispan.server.jgroups.spi.ChannelFactory;
import org.infinispan.server.jgroups.spi.service.ChannelBuilder;
import org.infinispan.server.jgroups.spi.service.ChannelConnectorBuilder;
import org.infinispan.server.jgroups.spi.service.ChannelServiceName;
import org.infinispan.server.jgroups.spi.service.ChannelServiceNameFactory;
import org.infinispan.server.jgroups.spi.service.ProtocolStackServiceName;
import org.infinispan.server.jgroups.subsystem.JGroupsBindingFactory;
import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.dmr.ModelNode;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.msc.service.ServiceTarget;
import org.jgroups.JChannel;

/**
 * Add operation handler for /subsystem=infinispan/cache-container=*
 * @author Paul Ferraro
 * @author Tristan Tarrant
 * @author Richard Achmatowicz
 */
public class CacheContainerAddHandler extends AbstractAddStepHandler {

    CacheContainerAddHandler() {
        super(CacheContainerResource.CACHE_CONTAINER_ATTRIBUTES);
    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
        // Because we use child resources in a read-only manner to configure the cache container, replace the local model with the full model
        installRuntimeServices(context, operation, Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS)));
    }

    @Override
    protected Resource createResource(OperationContext context) {
        //HEALTH is a synthetic resource - we don't have it in XML but we would like to include it in the runtime
        Resource resource = super.createResource(context);
        PathAddress healthAddress = context.getCurrentAddress().append(ModelKeys.HEALTH, ModelKeys.HEALTH_NAME);
        context.addStep(Util.createAddOperation(healthAddress), new ReloadRequiredAddStepHandler(), OperationContext.Stage.MODEL);
        return resource;
    }

    static void installRuntimeServices(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
        String name = context.getCurrentAddressValue();

        ServiceTarget target = context.getServiceTarget();

        // pick up the attribute values from the model
        // make default cache non required (AS7-3488)
        String defaultCache = ModelNodes.asString(CacheContainerResource.DEFAULT_CACHE.resolveModelAttribute(context, model));
        String jndiName = ModelNodes.asString(CacheContainerResource.JNDI_NAME.resolveModelAttribute(context, model));
        ModuleIdentifier module = ModelNodes.asModuleIdentifier(CacheContainerResource.CACHE_CONTAINER_MODULE.resolveModelAttribute(context, model));

        CacheContainerConfigurationBuilder configBuilder = new CacheContainerConfigurationBuilder(name)
                .setModule(module)
                .setStatisticsEnabled(CacheContainerResource.STATISTICS.resolveModelAttribute(context, model).asBoolean());

        if (model.hasDefined(TransportResource.TRANSPORT_PATH.getKey())) {
            ModelNode transport = model.get(TransportResource.TRANSPORT_PATH.getKeyValuePair());
            String channel = ModelNodes.asString(TransportResource.CHANNEL.resolveModelAttribute(context, transport), ChannelServiceNameFactory.DEFAULT_CHANNEL);

            TransportConfigurationBuilder transportBuilder = configBuilder.setTransport()
                    .setLockTimeout(TransportResource.LOCK_TIMEOUT.resolveModelAttribute(context, transport).asLong(), TimeUnit.MILLISECONDS)
                    .setStrictPeerToPeer(TransportResource.STRICT_PEER_TO_PEER.resolveModelAttribute(context, transport).asBoolean());

            if (transport.hasDefined(ModelKeys.INITIAL_CLUSTER_SIZE)) {
                transportBuilder.setInitialClusterSize(TransportResource.INITIAL_CLUSTER_SIZE.resolveModelAttribute(context, transport).asInt());
            }

            if (transport.hasDefined(ModelKeys.INITIAL_CLUSTER_TIMEOUT)) {
                transportBuilder.setInitialClusterTimeout(TransportResource.INITIAL_CLUSTER_TIMEOUT.resolveModelAttribute(context, transport).asLong());
            }

            transportBuilder.build(target).install();

            if (!name.equals(channel)) {
                new BinderServiceBuilder<>(JGroupsBindingFactory.createChannelBinding(name), ChannelServiceName.CHANNEL.getServiceName(name), JChannel.class).build(target).install();

                new ChannelBuilder(name).build(target).install();
                new ChannelConnectorBuilder(name).build(target).install();
                new AliasServiceBuilder<>(ChannelServiceName.FACTORY.getServiceName(name), ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(channel), ChannelFactory.class).build(target).install();
            }
        }

        if (model.hasDefined(GlobalStateResource.GLOBAL_STATE_PATH.getKey())) {
            ModelNode globalState = model.get(GlobalStateResource.GLOBAL_STATE_PATH.getKeyValuePair());
            final String defaultPersistentLocation = InfinispanExtension.SUBSYSTEM_NAME + File.separatorChar + name;
            GlobalStateLocationConfigurationBuilder globalStateBuilder = configBuilder.setGlobalState();
            if (globalState.hasDefined(ModelKeys.PERSISTENT_LOCATION)) {
                ModelNode persistentLocation = globalState.get(ModelKeys.PERSISTENT_LOCATION);
                final String path = ModelNodes.asString(GlobalStateResource.PATH.resolveModelAttribute(context, persistentLocation), defaultPersistentLocation);
                final String relativeTo = ModelNodes.asString(GlobalStateResource.PERSISTENT_RELATIVE_TO.resolveModelAttribute(context, persistentLocation));
                globalStateBuilder.setPersistencePath(path).setPersistenceRelativeTo(relativeTo);
            } else {
                globalStateBuilder.setPersistencePath(defaultPersistentLocation).setPersistenceRelativeTo(ServerEnvironment.SERVER_DATA_DIR);
            }
            if (globalState.hasDefined(ModelKeys.SHARED_PERSISTENT_LOCATION)) {
                ModelNode persistentLocation = globalState.get(ModelKeys.SHARED_PERSISTENT_LOCATION);
                final String path = ModelNodes.asString(GlobalStateResource.SHARED_PERSISTENT_LOCATION_PATH.resolveModelAttribute(context, persistentLocation), defaultPersistentLocation);
                final String relativeTo = ModelNodes.asString(GlobalStateResource.SHARED_PERSISTENT_RELATIVE_TO.resolveModelAttribute(context, persistentLocation));
                globalStateBuilder.setSharedPersistencePath(path).setSharedPersistenceRelativeTo(relativeTo);
            } else {
                globalStateBuilder.setSharedPersistencePath(defaultPersistentLocation).setSharedPersistenceRelativeTo(ServerEnvironment.SERVER_DATA_DIR);
            }
            if (globalState.hasDefined(ModelKeys.TEMPORARY_LOCATION)) {
                ModelNode persistentLocation = globalState.get(ModelKeys.TEMPORARY_LOCATION);
                final String path = ModelNodes.asString(GlobalStateResource.TEMPORARY_STATE_PATH.resolveModelAttribute(context, persistentLocation), defaultPersistentLocation);
                final String relativeTo = ModelNodes.asString(GlobalStateResource.TEMPORARY_RELATIVE_TO.resolveModelAttribute(context, persistentLocation));
                globalStateBuilder.setTemporaryPath(path).setTemporaryRelativeTo(relativeTo);
            } else {
                globalStateBuilder.setTemporaryPath(defaultPersistentLocation).setTemporaryRelativeTo(ServerEnvironment.SERVER_TEMP_DIR);
            }
            ConfigurationStorage configurationStorage = ConfigurationStorage.valueOf(GlobalStateResource.CONFIGURATION_STORAGE.resolveModelAttribute(context, globalState).asString());
            globalStateBuilder.setConfigurationStorage(configurationStorage);
            if (configurationStorage.equals(ConfigurationStorage.MANAGED)) {
                switch (context.getProcessType()) {
                    case STANDALONE_SERVER:
                        globalStateBuilder.setConfigurationStorageClass(StandaloneServerLocalConfigurationStorage.class.getName());
                        break;
                    case DOMAIN_SERVER:
                        InfinispanLogger.ROOT_LOGGER.managedConfigurationUnavailableInDomainMode();
                        break;
                    default:
                        // No need
                        break;
                }
            } else if (configurationStorage.equals(ConfigurationStorage.CUSTOM)) {
                globalStateBuilder.setConfigurationStorageClass(GlobalStateResource.CONFIGURATION_STORAGE_CLASS.resolveModelAttribute(context, globalState).asString());
            }
        }

        if (model.hasDefined(ModelKeys.SECURITY) && model.get(ModelKeys.SECURITY).hasDefined(ModelKeys.SECURITY_NAME)) {
            ModelNode securityModel = model.get(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME);

            if (securityModel.hasDefined(ModelKeys.AUTHORIZATION) && securityModel.get(ModelKeys.AUTHORIZATION).hasDefined(ModelKeys.AUTHORIZATION_NAME)) {
                ModelNode authzModel = securityModel.get(ModelKeys.AUTHORIZATION, ModelKeys.AUTHORIZATION_NAME);

                AuthorizationConfigurationBuilder authorizationConfig = configBuilder.setAuthorization();
                if (authzModel.hasDefined(ModelKeys.AUDIT_LOGGER)) {
                   authorizationConfig.setAuditLogger(ModelNodes.asString(CacheContainerAuthorizationResource.AUDIT_LOGGER.resolveModelAttribute(context, authzModel)));
                }
                authorizationConfig.setPrincipalMapper(ModelNodes.asString(CacheContainerAuthorizationResource.MAPPER.resolveModelAttribute(context, authzModel)));

                for(ModelNode roleNode : authzModel.get(ModelKeys.ROLE).asList()) {
                    ModelNode role = roleNode.get(0);
                    String roleName = AuthorizationRoleResource.NAME.resolveModelAttribute(context, role).asString();
                    List<String> permissions = new ArrayList<>();
                    for(ModelNode permission : AuthorizationRoleResource.PERMISSIONS.resolveModelAttribute(context, role).asList()) {
                        permissions.add(permission.asString());
                    }
                    authorizationConfig.getRoles().put(roleName, permissions);
                }
            }
        }

        if (model.hasDefined(ModelKeys.MODULES) && model.get(ModelKeys.MODULES).hasDefined(ModelKeys.MODULES_NAME)) {
            ModelNode modulesModel = model.get(ModelKeys.MODULES, ModelKeys.MODULES_NAME);
            List<ModelNode> moduleListNodes = modulesModel.get(ModelKeys.MODULE).asList();
            List<ModuleIdentifier> modules = new ArrayList<>(moduleListNodes.size());
            for (ModelNode moduleListNode : moduleListNodes) {
                ModelNode moduleNode = moduleListNode.get(0);
                String moduleName = CacheContainerModuleResource.NAME.resolveModelAttribute(context, moduleNode).asString();
                String moduleSlot = moduleNode.hasDefined(ModelKeys.SLOT) ? CacheContainerModuleResource.SLOT.resolveModelAttribute(context, moduleNode).asString() : null;
                modules.add(ModuleIdentifier.create(moduleName, moduleSlot));
            }
            configBuilder.setModules(modules);
        }

        if (model.hasDefined(ModelKeys.COUNTERS)) {
            ModelNode countersModel = model.get(ModelKeys.COUNTERS, ModelKeys.COUNTERS_NAME);
            CounterManagerConfigurationBuilder counterManagerConfigurationBuilder = configBuilder.setCounterManagerConfiguration();
            counterManagerConfigurationBuilder.setNumOwners(CacheContainerCountersResource.NUM_OWNERS.resolveModelAttribute(context, countersModel).asInt());
            counterManagerConfigurationBuilder.setReliability(Reliability.valueOf(CacheContainerCountersResource.RELIABILITY.resolveModelAttribute(context, countersModel).asString()));
        }

        // Install cache container configuration service
        configBuilder.build(target).install();

        // Install cache container service
        CacheContainerBuilder containerBuilder = new CacheContainerBuilder(name, defaultCache);

        if (model.hasDefined(CacheContainerResource.ALIASES.getName())) {
            for (ModelNode alias : operation.get(CacheContainerResource.ALIASES.getName()).asList()) {
                containerBuilder.addAlias(alias.asString());
            }
        }

        containerBuilder.build(target).install();

        // Install cache container jndi binding
        ContextNames.BindInfo binding = InfinispanBindingFactory.createCacheContainerBinding(name);
        BinderServiceBuilder<CacheContainer> bindingBuilder = new BinderServiceBuilder<>(binding, CacheContainerServiceName.CACHE_CONTAINER.getServiceName(name), CacheContainer.class);
        if (jndiName != null) {
            bindingBuilder.alias(ContextNames.bindInfoFor(JndiNameFactory.parse(jndiName).getAbsoluteName()));
        }
        bindingBuilder.build(target).install();

        if ((defaultCache != null) && !defaultCache.equals(CacheServiceNameFactory.DEFAULT_CACHE)) {

            for (CacheServiceNameFactory nameFactory : CacheServiceName.values()) {
                new AliasServiceBuilder<>(nameFactory.getServiceName(name), nameFactory.getServiceName(name, defaultCache), Object.class).build(target).install();
            }

            new BinderServiceBuilder<>(InfinispanBindingFactory.createCacheBinding(name, CacheServiceNameFactory.DEFAULT_CACHE), CacheServiceName.CACHE.getServiceName(name), Cache.class).build(target).install();
        }
    }

    static void removeRuntimeServices(OperationContext context, ModelNode operation, ModelNode model) {
        String name = context.getCurrentAddressValue();

        // remove the BinderService entry
        context.removeService(InfinispanBindingFactory.createCacheContainerBinding(name).getBinderServiceName());

        for (CacheContainerServiceNameFactory factory : CacheContainerServiceName.values()) {
            context.removeService(factory.getServiceName(name));
        }

        if (model.hasDefined(TransportResource.TRANSPORT_PATH.getKey())) {
            context.removeService(new TransportConfigurationBuilder(name).getServiceName());

            context.removeService(JGroupsBindingFactory.createChannelBinding(name).getBinderServiceName());
            for (ChannelServiceNameFactory factory : ChannelServiceName.values()) {
                context.removeService(factory.getServiceName(name));
            }
        }
    }
}
