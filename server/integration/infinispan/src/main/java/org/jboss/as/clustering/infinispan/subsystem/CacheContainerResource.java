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

import java.time.format.DateTimeFormatter;

import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ListAttributeDefinition;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.ResourceDefinition;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleListAttributeDefinition;
import org.jboss.as.controller.SimpleMapAttributeDefinition;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.operations.validation.IntRangeValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class CacheContainerResource extends SimpleResourceDefinition {

    public static final PathElement CONTAINER_PATH = PathElement.pathElement(ModelKeys.CACHE_CONTAINER);

    // attributes
    static final AttributeDefinition ALIAS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.ALIAS, ModelType.STRING, true)
                    .setXmlName(Attribute.NAME.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleListAttributeDefinition ALIASES = SimpleListAttributeDefinition.Builder.of(ModelKeys.ALIASES, ALIAS).
            setAllowNull(true).
            build();

    static final SimpleAttributeDefinition CACHE_CONTAINER_MODULE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MODULE, ModelType.STRING, true)
                    .setXmlName(Attribute.MODULE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setValidator(new ModuleIdentifierValidator(true))
                    .setDefaultValue(new ModelNode().set("org.infinispan.extension"))
                    .build();

    // make default-cache non required (AS7-3488)
    static final SimpleAttributeDefinition DEFAULT_CACHE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.DEFAULT_CACHE, ModelType.STRING, true)
                    .setXmlName(Attribute.DEFAULT_CACHE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition JNDI_NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.JNDI_NAME, ModelType.STRING, true)
                    .setXmlName(Attribute.JNDI_NAME.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, true)
                    .setXmlName(Attribute.NAME.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition START =
            new SimpleAttributeDefinitionBuilder(ModelKeys.START, ModelType.STRING, true)
                    .setXmlName(Attribute.START.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setValidator(new EnumValidator<>(StartMode.class, true, false))
                    .setDefaultValue(new ModelNode().set(StartMode.LAZY.name()))
                    .build();

    static final SimpleAttributeDefinition STATISTICS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.STATISTICS, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.STATISTICS.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(true))
                    .build();

   static final SimpleAttributeDefinition CLUSTER_REBALANCE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.CLUSTER_REBALANCE, ModelType.BOOLEAN, true)
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_NONE)
               .setDefaultValue(new ModelNode().set(true))
               .setStorageRuntime()
               .build();

    static final AttributeDefinition[] CACHE_CONTAINER_ATTRIBUTES = {DEFAULT_CACHE, ALIASES, JNDI_NAME, START, CACHE_CONTAINER_MODULE, STATISTICS};

    // operations
    static final OperationDefinition ALIAS_ADD = new SimpleOperationDefinitionBuilder("add-alias", new InfinispanResourceDescriptionResolver("cache-container.alias"))
            .setParameters(NAME)
            .build();

    static final OperationDefinition ALIAS_REMOVE = new SimpleOperationDefinitionBuilder("remove-alias", new InfinispanResourceDescriptionResolver("cache-container.alias"))
            .setParameters(NAME)
            .build();

    static final ListAttributeDefinition PROTO_URLS =
         new StringListAttributeDefinition.Builder("file-urls")
               .build();

    static final SimpleAttributeDefinition PROTO_NAME =
           new SimpleAttributeDefinition("file-name", ModelType.STRING, false);

    static final ListAttributeDefinition PROTO_NAMES =
           new StringListAttributeDefinition.Builder("file-names")
                   .build();

    static final ListAttributeDefinition PROTO_CONTENTS =
           new StringListAttributeDefinition.Builder("file-contents")
                   .build();

    static final OperationDefinition GET_PROTO_NAMES = new SimpleOperationDefinitionBuilder("get-proto-schema-names", new InfinispanResourceDescriptionResolver("cache-container"))
           .setRuntimeOnly()
           .build();

    static final OperationDefinition GET_PROTO = new SimpleOperationDefinitionBuilder("get-proto-schema", new InfinispanResourceDescriptionResolver("cache-container"))
           .setParameters(PROTO_NAME)
           .setRuntimeOnly()
           .build();

    static final OperationDefinition GET_PROTO_SCHEMAS_WITH_ERRORS = new SimpleOperationDefinitionBuilder("get-proto-schemas-with-errors", new InfinispanResourceDescriptionResolver("cache-container"))
           .setRuntimeOnly()
           .build();

    static final OperationDefinition GET_PROTO_SCHEMA_ERRORS = new SimpleOperationDefinitionBuilder("get-proto-schema-errors", new InfinispanResourceDescriptionResolver("cache-container"))
           .setParameters(PROTO_NAME)
           .setRuntimeOnly()
           .build();

    static final OperationDefinition UPLOAD_PROTO = new SimpleOperationDefinitionBuilder("upload-proto-schemas", new InfinispanResourceDescriptionResolver("cache-container"))
           .setParameters(PROTO_NAMES, PROTO_URLS)
           .setRuntimeOnly()
           .build();

    static final OperationDefinition REGISTER_PROTO = new SimpleOperationDefinitionBuilder("register-proto-schemas", new InfinispanResourceDescriptionResolver("cache-container"))
           .setParameters(PROTO_NAMES, PROTO_CONTENTS)
           .setRuntimeOnly()
           .build();

    static final OperationDefinition UNREGISTER_PROTO = new SimpleOperationDefinitionBuilder("unregister-proto-schemas", new InfinispanResourceDescriptionResolver("cache-container"))
           .setParameters(PROTO_NAMES)
           .setRuntimeOnly()
           .build();

    static final OperationDefinition CLI_INTERPRETER = new SimpleOperationDefinitionBuilder("cli-interpreter", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
            .setRuntimeOnly()
           .build();

   static final SimpleAttributeDefinition SITE_NAME =
         new SimpleAttributeDefinitionBuilder("site-name", ModelType.STRING, false)
               .build();

   static final OperationDefinition BACKUP_TAKE_SITE_OFFLINE =
         new SimpleOperationDefinitionBuilder("take-site-offline", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
               .setParameters(SITE_NAME)
               .setRuntimeOnly()
               .build();

   static final OperationDefinition BACKUP_BRING_SITE_ONLINE =
         new SimpleOperationDefinitionBuilder("bring-site-online", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
               .setParameters(SITE_NAME)
               .setRuntimeOnly()
               .build();

   static final OperationDefinition BACKUP_PUSH_STATE =
         new SimpleOperationDefinitionBuilder("push-state", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
               .setParameters(SITE_NAME)
               .setRuntimeOnly()
               .build();

   static final OperationDefinition BACKUP_CANCEL_PUSH_STATE =
         new SimpleOperationDefinitionBuilder("cancel-push-state", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
               .setParameters(SITE_NAME)
               .setRuntimeOnly()
               .build();

   static final SimpleAttributeDefinition COUNT = SimpleAttributeDefinitionBuilder.create("lines", ModelType.INT, true)
           .setAllowExpression(true)
           .setDefaultValue(new ModelNode(10))
           .setValidator(new IntRangeValidator(-1, true))
           .build();

   static final SimpleAttributeDefinition SINCE = SimpleAttributeDefinitionBuilder.create("since", ModelType.STRING, true)
           .setAllowExpression(true)
           .setValidator(new DateTimeValidator(DateTimeFormatter.ISO_DATE_TIME, true, true))
           .build();

   static final SimpleAttributeDefinition CATEGORY = SimpleAttributeDefinitionBuilder.create("category", ModelType.STRING, true)
           .setAllowExpression(true)
           .setValidator(new EnumValidator<>(EventLogCategory.class, true, true))
           .build();

   static final SimpleAttributeDefinition LEVEL = SimpleAttributeDefinitionBuilder.create("level", ModelType.STRING, true)
           .setAllowExpression(true)
           .setValidator(new EnumValidator<>(EventLogLevel.class, true, true))
           .build();

   static final OperationDefinition READ_EVENT_LOG =
           new SimpleOperationDefinitionBuilder("read-event-log", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
               .setParameters(COUNT, SINCE, CATEGORY, LEVEL)
               .setReplyType(ModelType.LIST)
               .setReplyValueType(ModelType.OBJECT)
               .setReadOnly()
               .setRuntimeOnly()
               .build();

    static final OperationDefinition TASK_LIST =
           new SimpleOperationDefinitionBuilder("task-list", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
                .setReplyType(ModelType.LIST)
                .setReplyValueType(ModelType.OBJECT)
                .setReadOnly()
                .setRuntimeOnly()
                .build();

    static final SimpleAttributeDefinition TASK_NAME = SimpleAttributeDefinitionBuilder.create("name", ModelType.STRING, false)
           .setAllowExpression(true)
           .build();

    static final SimpleAttributeDefinition TASK_CACHE_NAME = SimpleAttributeDefinitionBuilder.create("cache-name", ModelType.STRING, true)
           .setAllowExpression(true)
           .build();

    static final SimpleMapAttributeDefinition TASK_PARAMETERS = new SimpleMapAttributeDefinition.Builder("parameters", ModelType.STRING, true)
           .setAllowExpression(true)
           .build();

    static final SimpleAttributeDefinition TASK_ASYNC = SimpleAttributeDefinitionBuilder.create("async", ModelType.BOOLEAN, true)
           .setAllowExpression(true)
           .setDefaultValue(new ModelNode(false))
           .build();

    static final OperationDefinition TASK_EXECUTE =
           new SimpleOperationDefinitionBuilder("task-execute", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
                .setParameters(TASK_NAME, TASK_CACHE_NAME, TASK_PARAMETERS, TASK_ASYNC)
                .setReplyType(ModelType.LIST)
                .setReplyValueType(ModelType.OBJECT)
                .setReadOnly()
                .setRuntimeOnly()
                .build();

    static final OperationDefinition TASK_STATUS =
            new SimpleOperationDefinitionBuilder("task-status", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
                    .setReplyType(ModelType.LIST)
                    .setReplyValueType(ModelType.OBJECT)
                    .setReadOnly()
                    .setRuntimeOnly()
                    .build();

    static final SimpleAttributeDefinition SCRIPT_NAME = SimpleAttributeDefinitionBuilder.create("name", ModelType.STRING, false)
            .setAllowExpression(true)
            .build();

    static final SimpleAttributeDefinition SCRIPT_CODE = SimpleAttributeDefinitionBuilder.create("code", ModelType.STRING, false)
            .setAllowExpression(true)
            .build();

    static final OperationDefinition SCRIPT_ADD = new SimpleOperationDefinitionBuilder("script-add", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
            .setParameters(SCRIPT_NAME, SCRIPT_CODE)
            .setRuntimeOnly()
            .build();

    static final OperationDefinition SCRIPT_CAT = new SimpleOperationDefinitionBuilder("script-cat", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
            .setParameters(SCRIPT_NAME)
            .setReplyType(ModelType.STRING)
            .setReadOnly()
            .setRuntimeOnly()
            .build();

    static final OperationDefinition SCRIPT_REMOVE = new SimpleOperationDefinitionBuilder("script-remove", new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
            .setParameters(SCRIPT_NAME)
            .setRuntimeOnly()
            .build();

   static final SimpleAttributeDefinition BOOL_VALUE = SimpleAttributeDefinitionBuilder.create(ModelKeys.VALUE, ModelType.BOOLEAN, false)
         .setDefaultValue(new ModelNode(true))
         .build();

   static final OperationDefinition CLUSTER_REBALANCE_OPERATION = new SimpleOperationDefinitionBuilder(ModelKeys.CLUSTER_REBALANCE, new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER))
         .setParameters(BOOL_VALUE)
         .setRuntimeOnly()
         .build();


    private final ResolvePathHandler resolvePathHandler;
    private final PathManager pathManager;
    private final boolean runtimeRegistration;
    public CacheContainerResource(final ResolvePathHandler resolvePathHandler, PathManager pathManager, boolean runtimeRegistration) {
        super(CONTAINER_PATH,
                new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER),
                new CacheContainerAddHandler(),
                new CacheContainerRemoveHandler());
        this.resolvePathHandler = resolvePathHandler;
        this.pathManager = pathManager;
        this.runtimeRegistration = runtimeRegistration;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // the handlers need to take account of alias
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(CACHE_CONTAINER_ATTRIBUTES);
        for (AttributeDefinition attr : CACHE_CONTAINER_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }

        if (runtimeRegistration) {
            resourceRegistration.registerReadWriteAttribute(CLUSTER_REBALANCE, ClusterRebalanceAttributeHandler.INSTANCE, ClusterRebalanceAttributeHandler.INSTANCE);
            // register runtime cache container read-only metrics (attributes and handlers)
            CacheContainerMetricsHandler.INSTANCE.registerMetrics(resourceRegistration);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        // register add-alias and remove-alias
        resourceRegistration.registerOperationHandler(ALIAS_ADD, AddAliasCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(ALIAS_REMOVE, RemoveAliasCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(GET_PROTO_NAMES, GetProtobufSchemaNamesHandler.INSTANCE);
        resourceRegistration.registerOperationHandler(GET_PROTO, GetProtobufSchemaHandler.INSTANCE);
        resourceRegistration.registerOperationHandler(GET_PROTO_SCHEMAS_WITH_ERRORS, GetProtobufSchemasWithErrorsHandler.INSTANCE);
        resourceRegistration.registerOperationHandler(GET_PROTO_SCHEMA_ERRORS, GetProtoSchemaErrorsHandler.INSTANCE);
        resourceRegistration.registerOperationHandler(UPLOAD_PROTO, UploadProtoFileOperationHandler.INSTANCE);
        resourceRegistration.registerOperationHandler(REGISTER_PROTO, RegisterProtoSchemasOperationHandler.INSTANCE);
        resourceRegistration.registerOperationHandler(UNREGISTER_PROTO, UnregisterProtoSchemasOperationHandler.INSTANCE);
        resourceRegistration.registerOperationHandler(CLI_INTERPRETER, CliInterpreterHandler.INSTANCE);
        resourceRegistration.registerOperationHandler(BACKUP_TAKE_SITE_OFFLINE, CacheContainerCommands.BackupTakeSiteOfflineCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(BACKUP_BRING_SITE_ONLINE, CacheContainerCommands.BackupBringSiteOnlineCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(BACKUP_PUSH_STATE, CacheContainerCommands.BackupPushStateCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(BACKUP_CANCEL_PUSH_STATE, CacheContainerCommands.BackupCancelPushStateCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(READ_EVENT_LOG, CacheContainerCommands.ReadEventLogCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(SCRIPT_ADD, CacheContainerCommands.ScriptAddCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(SCRIPT_CAT, CacheContainerCommands.ScriptCatCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(SCRIPT_REMOVE, CacheContainerCommands.ScriptRemoveCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(TASK_LIST, CacheContainerCommands.TaskListCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(TASK_EXECUTE, CacheContainerCommands.TaskExecuteCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(TASK_STATUS, CacheContainerCommands.TaskStatusCommand.INSTANCE);
        resourceRegistration.registerOperationHandler(CLUSTER_REBALANCE_OPERATION, CacheContainerCommands.ClusterRebalanceCommand.INSTANCE);
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);

        // child resources
        resourceRegistration.registerSubModel(new HealthResource(pathManager, runtimeRegistration));

        resourceRegistration.registerSubModel(new TransportResource());
        resourceRegistration.registerSubModel(new CacheContainerSecurityResource());
        resourceRegistration.registerSubModel(new CacheContainerModulesResource());
        resourceRegistration.registerSubModel(new GlobalStateResource());
        for(ResourceDefinition resource : ThreadPoolResource.values()) {
            resourceRegistration.registerSubModel(resource);
        }
        for(ResourceDefinition resource : ScheduledThreadPoolResource.values()) {
            resourceRegistration.registerSubModel(resource);
        }
        resourceRegistration.registerSubModel(new CacheContainerConfigurationsResource(resolvePathHandler, runtimeRegistration));
        resourceRegistration.registerSubModel(new LocalCacheResource(resolvePathHandler, runtimeRegistration));
        resourceRegistration.registerSubModel(new InvalidationCacheResource(resolvePathHandler, runtimeRegistration));
        resourceRegistration.registerSubModel(new ReplicatedCacheResource(resolvePathHandler, runtimeRegistration));
        resourceRegistration.registerSubModel(new DistributedCacheResource(resolvePathHandler, runtimeRegistration));
        resourceRegistration.registerSubModel(new CacheContainerCountersResource(resolvePathHandler, runtimeRegistration));
    }
}
