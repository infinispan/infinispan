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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;

import org.infinispan.commons.util.Util;
import org.jboss.as.controller.*;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.OperationEntry;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Base class for store resources which require common store attributes only.
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Tristan Tarrant
 */
public class BaseStoreResource extends BaseLoaderResource {

    // attributes
    static final SimpleAttributeDefinition FETCH_STATE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.FETCH_STATE, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.FETCH_STATE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(true))
                    .build();
    static final SimpleAttributeDefinition PASSIVATION =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PASSIVATION, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.PASSIVATION.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(true))
                    .build();
    static final SimpleAttributeDefinition PURGE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PURGE, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.PURGE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(true))
                    .build();
    static final SimpleAttributeDefinition READ_ONLY =
            new SimpleAttributeDefinitionBuilder(ModelKeys.READ_ONLY, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.READ_ONLY.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();
    static final SimpleAttributeDefinition SINGLETON =
            new SimpleAttributeDefinitionBuilder(ModelKeys.SINGLETON, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.SINGLETON.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();

    static final AttributeDefinition[] COMMON_STORE_ATTRIBUTES = {PASSIVATION, FETCH_STATE, PURGE, READ_ONLY, SINGLETON};
    /* Note this has loader attributes as well */
    static final AttributeDefinition[] COMMON_STORE_PARAMETERS = {SHARED, PRELOAD, PASSIVATION, FETCH_STATE, PURGE, READ_ONLY, SINGLETON, PROPERTIES};

    // operations
    private static final OperationDefinition CACHE_STORE_ADD_DEFINITION = new SimpleOperationDefinitionBuilder(ADD, new InfinispanResourceDescriptionResolver(ModelKeys.STORE))
        .setParameters(COMMON_STORE_PARAMETERS)
        .build();

    public BaseStoreResource(PathElement path, String resourceKey, CacheResource cacheResource, AttributeDefinition[] attributes) {
        super(path, resourceKey, cacheResource, Util.arrayConcat(COMMON_STORE_ATTRIBUTES,  attributes));
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);
        // child resources
        resourceRegistration.registerSubModel(new StoreWriteBehindResource());
    }

    // override the add operation to provide a custom definition (for the optional PROPERTIES parameter to add())
    @Override
    protected void registerAddOperation(final ManagementResourceRegistration registration, final AbstractAddStepHandler handler, OperationEntry.Flag... flags) {
        registration.registerOperationHandler(CACHE_STORE_ADD_DEFINITION, handler);
    }
}
