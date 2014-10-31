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

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.AttributeMarshaller;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleMapAttributeDefinition;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.dmr.Property;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Base class for cache resources which require common cache attributes only.
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class CacheResource extends SimpleResourceDefinition {

    // attributes
    static final SimpleAttributeDefinition BATCHING =
            new SimpleAttributeDefinitionBuilder(ModelKeys.BATCHING, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.BATCHING.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();

    static final SimpleAttributeDefinition CACHE_MODULE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MODULE, ModelType.STRING, true)
                    .setXmlName(Attribute.MODULE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setValidator(new ModuleIdentifierValidator(true))
                    .build();

    static final SimpleAttributeDefinition INDEXING =
            new SimpleAttributeDefinitionBuilder(ModelKeys.INDEXING, ModelType.STRING, true)
                    .setXmlName(Attribute.INDEX.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setValidator(new EnumValidator<Indexing>(Indexing.class, true, false))
                    .setDefaultValue(new ModelNode().set(Indexing.NONE.name()))
                    .build();

    static final SimpleAttributeDefinition INDEXING_AUTO_CONFIG =
            new SimpleAttributeDefinitionBuilder(ModelKeys.AUTO_CONFIG, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.AUTO_CONFIG.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();

    static final SimpleMapAttributeDefinition INDEXING_PROPERTIES = new SimpleMapAttributeDefinition.Builder(ModelKeys.INDEXING_PROPERTIES, true)
            .setAllowExpression(true)
            .setAttributeMarshaller(new AttributeMarshaller() {
                @Override
                public void marshallAsElement(AttributeDefinition attribute, ModelNode resourceModel, boolean marshallDefault, XMLStreamWriter writer) throws XMLStreamException {
                    resourceModel = resourceModel.get(attribute.getName());
                    if (!resourceModel.isDefined()) {
                        return;
                    }
                    for (Property property : resourceModel.asPropertyList()) {
                        writer.writeStartElement(org.jboss.as.controller.parsing.Element.PROPERTY.getLocalName());
                        writer.writeAttribute(org.jboss.as.controller.parsing.Element.NAME.getLocalName(), property.getName());
                        writer.writeCharacters(property.getValue().asString());
                        writer.writeEndElement();
                    }
                }
            })
            .build();

    static final SimpleAttributeDefinition JNDI_NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.JNDI_NAME, ModelType.STRING, true)
                    .setXmlName(Attribute.JNDI_NAME.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .build();

    static final SimpleAttributeDefinition START =
            new SimpleAttributeDefinitionBuilder(ModelKeys.START, ModelType.STRING, true)
                    .setXmlName(Attribute.START.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setValidator(new EnumValidator<StartMode>(StartMode.class, true, false))
                    .setDefaultValue(new ModelNode().set(StartMode.LAZY.name()))
                    .build();

    static final SimpleAttributeDefinition STATISTICS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.STATISTICS, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.STATISTICS.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();

    static final SimpleAttributeDefinition MIGRATOR_NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MIGRATOR_NAME, ModelType.STRING, true)
                   .setAllowExpression(false)
                   .build();

    static final SimpleAttributeDefinition REMOTE_CACHE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.REMOTE_CACHE, ModelType.STRING, true)
                   .setXmlName(Attribute.REMOTE_CACHE.getLocalName())
                   .setAllowExpression(false)
                   .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                   .build();

    static final SimpleAttributeDefinition REMOTE_SITE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.REMOTE_SITE, ModelType.STRING, true)
                   .setXmlName(Attribute.REMOTE_SITE.getLocalName())
                   .setAllowExpression(false)
                   .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                   .build();

    static final AttributeDefinition[] CACHE_ATTRIBUTES = {BATCHING, CACHE_MODULE, INDEXING, INDEXING_AUTO_CONFIG, INDEXING_PROPERTIES, JNDI_NAME, START, STATISTICS, REMOTE_CACHE, REMOTE_SITE};

    // here for legacy purposes only
    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, true)
                    .setXmlName(Attribute.NAME.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .build();

    // operations
    static final OperationDefinition CLEAR_CACHE =
            new SimpleOperationDefinitionBuilder("clear-cache",
                    InfinispanExtension.getResourceDescriptionResolver("cache")
            ).build();
    static final OperationDefinition STOP_CACHE =
            new SimpleOperationDefinitionBuilder("stop-cache",
                    InfinispanExtension.getResourceDescriptionResolver("cache")
            ).build();
    static final OperationDefinition START_CACHE =
            new SimpleOperationDefinitionBuilder("start-cache",
                    InfinispanExtension.getResourceDescriptionResolver("cache")
            ).build();
    static final OperationDefinition RESET_STATISTICS =
            new SimpleOperationDefinitionBuilder("reset-statistics",
                    InfinispanExtension.getResourceDescriptionResolver("cache")
            ).build();
    static final OperationDefinition RESET_ACTIVATION_STATISTICS =
            new SimpleOperationDefinitionBuilder(
                    "reset-activation-statistics",
                    InfinispanExtension.getResourceDescriptionResolver("cache")
            ).build();
    static final OperationDefinition RESET_INVALIDATION_STATISTICS =
            new SimpleOperationDefinitionBuilder(
                    "reset-invalidation-statistics",
                    InfinispanExtension.getResourceDescriptionResolver("cache")
            ).build();
    static final OperationDefinition RESET_PASSIVATION_STATISTICS =
            new SimpleOperationDefinitionBuilder(
                    "reset-passivation-statistics",
                    InfinispanExtension.getResourceDescriptionResolver("cache")
            ).build();
    static final OperationDefinition RESET_RPC_MANAGER_STATISTICS =
            new SimpleOperationDefinitionBuilder(
                    "reset-rpc-manager-statistics",
                    InfinispanExtension.getResourceDescriptionResolver("cache")
            ).build();

    static final OperationDefinition SYNCHRONIZE_DATA =
           new SimpleOperationDefinitionBuilder(
                   "synchronize-data",
                   InfinispanExtension.getResourceDescriptionResolver("cache")
           ).setParameters(MIGRATOR_NAME).build();

    static final OperationDefinition DISCONNECT_SOURCE =
           new SimpleOperationDefinitionBuilder(
                  "disconnect-source",
                  InfinispanExtension.getResourceDescriptionResolver("cache")
           ).setParameters(MIGRATOR_NAME).build();

    static final OperationDefinition RECORD_KNOWN_GLOBAL_KEYSET =
           new SimpleOperationDefinitionBuilder(
                  "record-known-global-keyset",
                  InfinispanExtension.getResourceDescriptionResolver("cache")
           ).build();

    static final OperationDefinition MASS_REINDEX =
          new SimpleOperationDefinitionBuilder(
                  "mass-reindex",
                  InfinispanExtension.getResourceDescriptionResolver("cache")
          ).build();


    protected final ResolvePathHandler resolvePathHandler;
    protected final boolean runtimeRegistration;
    public CacheResource(PathElement pathElement, ResourceDescriptionResolver descriptionResolver, AbstractAddStepHandler addHandler, OperationStepHandler removeHandler, ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
        super(pathElement, descriptionResolver, addHandler, removeHandler);
        this.resolvePathHandler = resolvePathHandler;
        this.runtimeRegistration = runtimeRegistration;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // do we really need a special handler here?
        final OperationStepHandler writeHandler = new CacheWriteAttributeHandler(CACHE_ATTRIBUTES);
        for (AttributeDefinition attr : CACHE_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, CacheReadAttributeHandler.INSTANCE, writeHandler);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        if (runtimeRegistration) {
            resourceRegistration.registerOperationHandler(CacheResource.CLEAR_CACHE, CacheCommands.ClearCacheCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.START_CACHE, CacheCommands.StartCacheCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.STOP_CACHE, CacheCommands.StopCacheCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.RESET_STATISTICS, CacheCommands.ResetCacheStatisticsCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.RESET_ACTIVATION_STATISTICS, CacheCommands.ResetActivationStatisticsCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.RESET_INVALIDATION_STATISTICS, CacheCommands.ResetInvalidationStatisticsCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.RESET_PASSIVATION_STATISTICS, CacheCommands.ResetPassivationStatisticsCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.RESET_RPC_MANAGER_STATISTICS, CacheCommands.ResetRpcManagerStatisticsCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.DISCONNECT_SOURCE, CacheCommands.DisconnectSourceCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.RECORD_KNOWN_GLOBAL_KEYSET, CacheCommands.RecordGlobalKeySetCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.SYNCHRONIZE_DATA, CacheCommands.SynchronizeDataCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CacheResource.MASS_REINDEX, CacheCommands.MassReindexCommand.INSTANCE);
        }
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);

        resourceRegistration.registerSubModel(new LockingResource());
        resourceRegistration.registerSubModel(new TransactionResource(runtimeRegistration));
        resourceRegistration.registerSubModel(new EvictionResource());
        resourceRegistration.registerSubModel(new ExpirationResource());
        resourceRegistration.registerSubModel(new CompatibilityResource());
        resourceRegistration.registerSubModel(new LoaderResource());
        resourceRegistration.registerSubModel(new ClusterLoaderResource());
        resourceRegistration.registerSubModel(new BackupSiteResource(runtimeRegistration));
        resourceRegistration.registerSubModel(new StoreResource());
        resourceRegistration.registerSubModel(new FileStoreResource(resolvePathHandler));
        resourceRegistration.registerSubModel(new StringKeyedJDBCStoreResource());
        resourceRegistration.registerSubModel(new BinaryKeyedJDBCStoreResource());
        resourceRegistration.registerSubModel(new MixedKeyedJDBCStoreResource());
        resourceRegistration.registerSubModel(new RemoteStoreResource());
        resourceRegistration.registerSubModel(new LevelDBStoreResource(resolvePathHandler));
        resourceRegistration.registerSubModel(new RestStoreResource());
        resourceRegistration.registerSubModel(new CacheSecurityResource());
    }
}
