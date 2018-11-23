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

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ModelVersion;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Base class for store resources which require common store attributes only.
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Tristan Tarrant
 */
public class BaseStoreConfigurationResource extends BaseLoaderConfigurationResource {

    // attributes
    static final SimpleAttributeDefinition FETCH_STATE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.FETCH_STATE, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.FETCH_STATE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(AbstractStoreConfiguration.FETCH_PERSISTENT_STATE.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition MAX_BATCH_SIZE =
          new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_BATCH_SIZE, ModelType.INT, true)
                .setXmlName(Attribute.MAX_BATCH_SIZE.getLocalName())
                .setAllowExpression(true)
                .setDefaultValue(new ModelNode().set(AbstractStoreConfiguration.MAX_BATCH_SIZE.getDefaultValue()))
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .build();
    static final SimpleAttributeDefinition PASSIVATION =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PASSIVATION, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.PASSIVATION.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(PersistenceConfiguration.PASSIVATION.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition PURGE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PURGE, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.PURGE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(AbstractStoreConfiguration.PURGE_ON_STARTUP.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition READ_ONLY =
            new SimpleAttributeDefinitionBuilder(ModelKeys.READ_ONLY, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.READ_ONLY.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(AbstractStoreConfiguration.IGNORE_MODIFICATIONS.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition SINGLETON =
            new SimpleAttributeDefinitionBuilder(ModelKeys.SINGLETON, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.SINGLETON.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .setDeprecated(ModelVersion.create(9, 0))
                    .build();

    static final AttributeDefinition[] BASE_STORE_ATTRIBUTES = {PASSIVATION, FETCH_STATE, PURGE, READ_ONLY, SINGLETON, MAX_BATCH_SIZE};
    /* Note this has loader attributes as well */
    static final AttributeDefinition[] BASE_STORE_PARAMETERS = {SHARED, SEGMENTED, PRELOAD, PASSIVATION, FETCH_STATE, PURGE, READ_ONLY, SINGLETON, MAX_BATCH_SIZE, PROPERTIES};


    public BaseStoreConfigurationResource(PathElement path, String resourceKey, CacheConfigurationResource parent,
                                          ManagementResourceRegistration containerReg, AttributeDefinition[] attributes) {
        super(path, resourceKey, parent, containerReg, Util.arrayConcat(BASE_STORE_ATTRIBUTES, attributes));
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);
        // child resources
        resourceRegistration.registerSubModel(new StoreWriteBehindResource());
    }
}
