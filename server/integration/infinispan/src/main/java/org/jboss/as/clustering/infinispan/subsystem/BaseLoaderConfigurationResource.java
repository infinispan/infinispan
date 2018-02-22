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
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleListAttributeDefinition;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Base class for loader resources which require common loader attributes only.
 *
 * @author William Burns (c) 2013 Red Hat Inc.
 */
public class BaseLoaderConfigurationResource extends CacheConfigurationChildResource {

    // attributes
    static final SimpleAttributeDefinition PRELOAD =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PRELOAD, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.PRELOAD.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();
    static final SimpleAttributeDefinition SHARED =
            new SimpleAttributeDefinitionBuilder(ModelKeys.SHARED, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.SHARED.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();

    // only used for reader and writer as name is part of model hierarchy
    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, true)
                    .setXmlName(Attribute.NAME.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    // used to pass in a list of properties to the loader add command
    static final AttributeDefinition PROPERTY = new SimpleAttributeDefinitionBuilder(ModelKeys.PROPERTY, ModelType.PROPERTY, true).build();
    static final SimpleListAttributeDefinition PROPERTIES = SimpleListAttributeDefinition.Builder.of(ModelKeys.PROPERTIES, PROPERTY).
            setRequired(false).
            build();

    static final AttributeDefinition[] BASE_LOADER_ATTRIBUTES = {SHARED, PRELOAD};
    static final AttributeDefinition[] BASE_LOADER_PARAMETERS = {SHARED, PRELOAD, PROPERTIES};

    // operations
    private static final OperationDefinition RESET_LOADER_STATISTICS =
         new SimpleOperationDefinitionBuilder(
               "reset-loader-statistics",
               new InfinispanResourceDescriptionResolver(ModelKeys.LOADER)
         ).setRuntimeOnly().build();

    public BaseLoaderConfigurationResource(PathElement path, String resourceKey, CacheConfigurationResource cacheResource, AttributeDefinition[] attributes) {
        super(path, resourceKey, cacheResource, Util.arrayConcat(BASE_LOADER_ATTRIBUTES, attributes));
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);
        resourceRegistration.registerSubModel(new LoaderPropertyResource(resource));
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        resourceRegistration.registerOperationHandler(BaseLoaderConfigurationResource.RESET_LOADER_STATISTICS,
                                                      CacheCommands.ResetCacheLoaderStatisticsCommand.INSTANCE);
    }
}
