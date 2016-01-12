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

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.AttributeMarshaller;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleMapAttributeDefinition;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.dmr.Property;

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
                    .setDefaultValue(new ModelNode().set(false))
                    .build();

    static final SimpleAttributeDefinition CACHE_MODULE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MODULE, ModelType.STRING, true)
                    .setXmlName(Attribute.MODULE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setValidator(new ModuleIdentifierValidator(true))
                    .build();

    static final SimpleAttributeDefinition CONFIGURATION =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CONFIGURATION, ModelType.STRING, true)
                    .setXmlName(Attribute.CONFIGURATION.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setAlternatives(ModelKeys.MODE)
                    .build();

    static final SimpleAttributeDefinition INDEXING =
            new SimpleAttributeDefinitionBuilder(ModelKeys.INDEXING, ModelType.STRING, true)
                    .setXmlName(Attribute.INDEX.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setValidator(new EnumValidator<>(Indexing.class, true, false))
                    .setDefaultValue(new ModelNode().set(Indexing.NONE.name()))
                    .build();

    static final SimpleAttributeDefinition INDEXING_AUTO_CONFIG =
            new SimpleAttributeDefinitionBuilder(ModelKeys.AUTO_CONFIG, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.AUTO_CONFIG.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
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

    static final SimpleAttributeDefinition INLINE_INTERCEPTORS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.INLINE_INTERCEPTORS, ModelType.BOOLEAN, true)
                     .setXmlName(Attribute.INLINE_INTERCEPTORS.getLocalName())
                     .setAllowExpression(false)
                     .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                     .setDefaultValue(new ModelNode().set(false))
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

    static final SimpleAttributeDefinition START =
            new SimpleAttributeDefinitionBuilder(ModelKeys.START, ModelType.STRING, true)
                    .setXmlName(Attribute.START.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setValidator(new EnumValidator<>(StartMode.class, true, false))
                    .setDefaultValue(new ModelNode().set(StartMode.EAGER.name()))
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

    static final AttributeDefinition[] ATTRIBUTES = {BATCHING, CACHE_MODULE, CONFIGURATION, INDEXING, INDEXING_AUTO_CONFIG, INDEXING_PROPERTIES, INLINE_INTERCEPTORS, JNDI_NAME, SIMPLE_CACHE, START, STATISTICS, STATISTICS_AVAILABLE, REMOTE_CACHE, REMOTE_SITE};

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

        final OperationStepHandler restartWriteHandler = new RestartCacheWriteAttributeHandler(getPathElement().getKey(), serviceInstaller, ATTRIBUTES);
        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, CacheReadAttributeHandler.INSTANCE, restartWriteHandler);
        }
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);

        resourceRegistration.registerSubModel(new LockingConfigurationResource(this));
        resourceRegistration.registerSubModel(new TransactionConfigurationResource(this));
        resourceRegistration.registerSubModel(new EvictionConfigurationResource(this));
        resourceRegistration.registerSubModel(new ExpirationConfigurationResource(this));
        resourceRegistration.registerSubModel(new CompatibilityConfigurationResource(this));
        resourceRegistration.registerSubModel(new LoaderConfigurationResource(this));
        resourceRegistration.registerSubModel(new ClusterLoaderConfigurationResource(this));
        resourceRegistration.registerSubModel(new BackupSiteConfigurationResource(this));
        resourceRegistration.registerSubModel(new StoreConfigurationResource(this));
        resourceRegistration.registerSubModel(new FileStoreResource(this));
        resourceRegistration.registerSubModel(new StringKeyedJDBCStoreResource(this));
        resourceRegistration.registerSubModel(new BinaryKeyedJDBCStoreConfigurationResource(this));
        resourceRegistration.registerSubModel(new MixedKeyedJDBCStoreConfigurationResource(this));
        resourceRegistration.registerSubModel(new RemoteStoreConfigurationResource(this));
        resourceRegistration.registerSubModel(new LevelDBStoreConfigurationResource(this));
        resourceRegistration.registerSubModel(new RestStoreConfigurationResource(this));
        resourceRegistration.registerSubModel(new CacheSecurityConfigurationResource(this));
    }

}
