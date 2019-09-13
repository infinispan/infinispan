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

import org.infinispan.configuration.cache.InvocationBatchingConfiguration;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
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
public class CacheConfigurationResource extends SimpleResourceDefinition implements RestartableResourceDefinition {

    // attributes
    static final SimpleAttributeDefinition BATCHING =
            new SimpleAttributeDefinitionBuilder(ModelKeys.BATCHING, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.BATCHING.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(InvocationBatchingConfiguration.ENABLED.getDefaultValue()))
                    .build();

    static final SimpleAttributeDefinition CONFIGURATION =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CONFIGURATION, ModelType.STRING, true)
                    .setXmlName(Attribute.CONFIGURATION.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setAlternatives(ModelKeys.MODE)
                    .build();

    static final SimpleAttributeDefinition JNDI_NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.JNDI_NAME, ModelType.STRING, true)
                    .setXmlName(Attribute.JNDI_NAME.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition SIMPLE_CACHE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.SIMPLE_CACHE, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.SIMPLE_CACHE.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();

    static final SimpleAttributeDefinition STATISTICS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.STATISTICS, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.STATISTICS.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();

   static final SimpleAttributeDefinition STATISTICS_AVAILABLE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.STATISTICS_AVAILABLE, ModelType.BOOLEAN, true)
               .setXmlName(Attribute.STATISTICS_AVAILABLE.getLocalName())
               .setAllowExpression(false)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(true))
               .build();

   static final SimpleAttributeDefinition TEMPLATE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.TEMPLATE, ModelType.BOOLEAN, false)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(true))
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

    static final AttributeDefinition[] ATTRIBUTES = {BATCHING, CONFIGURATION, JNDI_NAME, SIMPLE_CACHE, STATISTICS, STATISTICS_AVAILABLE, REMOTE_CACHE, REMOTE_SITE, TEMPLATE};

    // here for legacy purposes only
    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, true)
                    .setXmlName(Attribute.NAME.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    protected final ResolvePathHandler resolvePathHandler;
    protected final boolean runtimeRegistration;
    private final RestartableServiceHandler serviceInstaller;

    public CacheConfigurationResource(PathElement pathElement, ResourceDescriptionResolver descriptionResolver, CacheConfigurationAdd addHandler, OperationStepHandler removeHandler, ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
        super(pathElement, descriptionResolver, addHandler, removeHandler);
        this.serviceInstaller = addHandler;
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

        final OperationStepHandler restartWriteHandler = new RestartServiceWriteAttributeHandler(getPathElement().getKey(), serviceInstaller, CacheServiceName.CONFIGURATION, ATTRIBUTES);
        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, CacheReadAttributeHandler.INSTANCE, restartWriteHandler);
        }
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);

        resourceRegistration.registerSubModel(new LockingConfigurationResource(this));
        resourceRegistration.registerSubModel(new TransactionConfigurationResource(this));
        resourceRegistration.registerSubModel(new ExpirationConfigurationResource(this));
        resourceRegistration.registerSubModel(new MemoryBinaryConfigurationResource(this));
        resourceRegistration.registerSubModel(new MemoryObjectConfigurationResource(this));
        resourceRegistration.registerSubModel(new MemoryOffHeapConfigurationResource(this));
        resourceRegistration.registerSubModel(new CompatibilityConfigurationResource(this));
        resourceRegistration.registerSubModel(new IndexingConfigurationResource(this));
        resourceRegistration.registerSubModel(new BackupSiteConfigurationResource(this));
        resourceRegistration.registerSubModel(new CacheSecurityConfigurationResource(this));
        resourceRegistration.registerSubModel(new KeyDataTypeConfigurationResource(this));
        resourceRegistration.registerSubModel(new ValueDataTypeConfigurationResource(this));
        resourceRegistration.registerSubModel(new PersistenceConfigurationResource(resourceRegistration, this));
    }
}
