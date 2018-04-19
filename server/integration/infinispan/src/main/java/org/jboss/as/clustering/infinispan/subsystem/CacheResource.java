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

import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Base class for cache resources which require common cache attributes only.
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class CacheResource extends SimpleResourceDefinition implements RestartableResourceDefinition {

    // attributes
    static final SimpleAttributeDefinition CONFIGURATION =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CONFIGURATION, ModelType.STRING, true)
                    .setXmlName(Attribute.CONFIGURATION.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final AttributeDefinition[] CACHE_ATTRIBUTES = {CONFIGURATION};

    // here for legacy purposes only
    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, true)
                    .setXmlName(Attribute.NAME.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    // operation parameters
    static final SimpleAttributeDefinition MIGRATOR_NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MIGRATOR_NAME, ModelType.STRING, true)
                   .setAllowExpression(false)
                   .build();

   // operation parameters
   static final SimpleAttributeDefinition READ_BATCH =
           new SimpleAttributeDefinitionBuilder(ModelKeys.READ_BATCH, ModelType.INT, true)
                   .setAllowExpression(false)
                   .setDefaultValue(new ModelNode().set(1000))
                   .build();

   static final SimpleAttributeDefinition WRITE_THREADS =
           new SimpleAttributeDefinitionBuilder(ModelKeys.WRITE_THREADS, ModelType.INT, true)
                   .setAllowExpression(false)
                   .setDefaultValue(new ModelNode().set(ProcessorInfo.availableProcessors()))
                   .build();

    // operations
    static final OperationDefinition CLEAR_CACHE =
            new SimpleOperationDefinitionBuilder("clear-cache",
                    new InfinispanResourceDescriptionResolver("cache")
            ).setRuntimeOnly().build();
    static final OperationDefinition FLUSH_CACHE =
            new SimpleOperationDefinitionBuilder("flush-cache",
                    new InfinispanResourceDescriptionResolver("cache")
            ).setRuntimeOnly().build();
    static final OperationDefinition STOP_CACHE =
            new SimpleOperationDefinitionBuilder("stop-cache",
                    new InfinispanResourceDescriptionResolver("cache")
            ).setRuntimeOnly().build();
    static final OperationDefinition SHUTDOWN_CACHE =
         new SimpleOperationDefinitionBuilder("shutdown-cache",
               new InfinispanResourceDescriptionResolver("cache")
         ).setRuntimeOnly().build();
    static final OperationDefinition START_CACHE =
            new SimpleOperationDefinitionBuilder("start-cache",
                    new InfinispanResourceDescriptionResolver("cache")
            ).setRuntimeOnly().build();
    static final OperationDefinition RESET_STATISTICS =
            new SimpleOperationDefinitionBuilder("reset-statistics",
                    new InfinispanResourceDescriptionResolver("cache")
            ).setRuntimeOnly().build();
    static final OperationDefinition RESET_ACTIVATION_STATISTICS =
            new SimpleOperationDefinitionBuilder(
                    "reset-activation-statistics",
                    new InfinispanResourceDescriptionResolver("cache")
            ).setRuntimeOnly().build();
    static final OperationDefinition RESET_INVALIDATION_STATISTICS =
            new SimpleOperationDefinitionBuilder(
                    "reset-invalidation-statistics",
                    new InfinispanResourceDescriptionResolver("cache")
            ).setRuntimeOnly().build();
    static final OperationDefinition RESET_PASSIVATION_STATISTICS =
            new SimpleOperationDefinitionBuilder(
                    "reset-passivation-statistics",
                    new InfinispanResourceDescriptionResolver("cache")
            ).setRuntimeOnly().build();
    static final OperationDefinition RESET_RPC_MANAGER_STATISTICS =
            new SimpleOperationDefinitionBuilder(
                    "reset-rpc-manager-statistics",
                    new InfinispanResourceDescriptionResolver("cache")
            ).setRuntimeOnly().build();

    static final OperationDefinition SYNCHRONIZE_DATA =
           new SimpleOperationDefinitionBuilder(
                   "synchronize-data",
                   new InfinispanResourceDescriptionResolver("cache")
           ).setParameters(MIGRATOR_NAME, READ_BATCH, WRITE_THREADS).setRuntimeOnly().build();

    static final OperationDefinition DISCONNECT_SOURCE =
           new SimpleOperationDefinitionBuilder(
                  "disconnect-source",
                  new InfinispanResourceDescriptionResolver("cache")
           ).setParameters(MIGRATOR_NAME).setRuntimeOnly().build();

    static final OperationDefinition MASS_REINDEX =
          new SimpleOperationDefinitionBuilder(
                  "mass-reindex",
                  new InfinispanResourceDescriptionResolver("cache")
          ).setRuntimeOnly().build();


    protected final ResolvePathHandler resolvePathHandler;
    protected final boolean runtimeRegistration;
    private final RestartableServiceHandler serviceInstaller;

    public CacheResource(PathElement pathElement, ResourceDescriptionResolver descriptionResolver, CacheAdd cacheAddHandler, OperationStepHandler removeHandler, ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
        super(pathElement, descriptionResolver, cacheAddHandler, removeHandler);
        this.serviceInstaller = cacheAddHandler;
        this.resolvePathHandler = resolvePathHandler;
        this.runtimeRegistration = runtimeRegistration;
    }

    @Override
    public RestartableServiceHandler getServiceInstaller() {
        return serviceInstaller;
    }

    @Override
    public boolean isRuntimeRegistration() {
        return runtimeRegistration;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        final OperationStepHandler restartWriteHandler = new RestartServiceWriteAttributeHandler(getPathElement().getKey(), serviceInstaller, CacheServiceName.CACHE, CACHE_ATTRIBUTES);
        for (AttributeDefinition attr : CACHE_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, CacheReadAttributeHandler.INSTANCE, restartWriteHandler);
        }
        if (runtimeRegistration) {
            CacheMetricsHandler.INSTANCE.registerCommonMetrics(resourceRegistration);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        resourceRegistration.registerOperationHandler(CacheResource.CLEAR_CACHE, CacheCommands.ClearCacheCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.FLUSH_CACHE, CacheCommands.FlushCacheCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.START_CACHE, CacheCommands.StartCacheCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.STOP_CACHE, CacheCommands.StopCacheCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.SHUTDOWN_CACHE, CacheCommands.ShudownCacheCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.RESET_STATISTICS, CacheCommands.ResetCacheStatisticsCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.RESET_ACTIVATION_STATISTICS, CacheCommands.ResetActivationStatisticsCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.RESET_INVALIDATION_STATISTICS, CacheCommands.ResetInvalidationStatisticsCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.RESET_PASSIVATION_STATISTICS, CacheCommands.ResetPassivationStatisticsCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.RESET_RPC_MANAGER_STATISTICS, CacheCommands.ResetRpcManagerStatisticsCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.DISCONNECT_SOURCE, CacheCommands.DisconnectSourceCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.SYNCHRONIZE_DATA, CacheCommands.SynchronizeDataCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CacheResource.MASS_REINDEX, CacheCommands.MassReindexCommand.INSTANCE);
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);
        resourceRegistration.registerSubModel(new TransactionResource(this));
        resourceRegistration.registerSubModel(new BackupSiteResource(this));
    }

}
