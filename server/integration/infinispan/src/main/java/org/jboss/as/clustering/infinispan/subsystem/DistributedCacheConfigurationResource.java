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

import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/distributed-cache=*
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Radoslav Husar
 */
public class DistributedCacheConfigurationResource extends SharedCacheConfigurationResource {

    public static final PathElement PATH = PathElement.pathElement(ModelKeys.DISTRIBUTED_CACHE_CONFIGURATION);

    // attributes
    static final SimpleAttributeDefinition L1_LIFESPAN =
            new SimpleAttributeDefinitionBuilder(ModelKeys.L1_LIFESPAN, ModelType.LONG, true)
                    .setXmlName(Attribute.L1_LIFESPAN.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(0))
                    .build();

    static final SimpleAttributeDefinition OWNERS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.OWNERS, ModelType.INT, true)
                    .setXmlName(Attribute.OWNERS.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(HashConfiguration.NUM_OWNERS.getDefaultValue()))
                    .build();

    static final SimpleAttributeDefinition SEGMENTS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.SEGMENTS, ModelType.INT, true)
                    .setXmlName(Attribute.SEGMENTS.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(HashConfiguration.NUM_SEGMENTS.getDefaultValue()))
                    .build();

    static final SimpleAttributeDefinition CAPACITY_FACTOR =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CAPACITY_FACTOR, ModelType.DOUBLE, true)
                    .setXmlName(Attribute.CAPACITY_FACTOR.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(HashConfiguration.CAPACITY_FACTOR.getDefaultValue()))
                    .build();

    static final AttributeDefinition[] ATTRIBUTES = {OWNERS, SEGMENTS, CAPACITY_FACTOR, L1_LIFESPAN};

    public DistributedCacheConfigurationResource(final ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
        super(PATH,
                new InfinispanResourceDescriptionResolver(ModelKeys.DISTRIBUTED_CACHE_CONFIGURATION),
                DistributedCacheConfigurationAdd.INSTANCE,
                new CacheConfigurationRemove(), resolvePathHandler, runtimeRegistration);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        final OperationStepHandler restartWriteHandler = new RestartServiceWriteAttributeHandler(getPathElement().getKey(), getServiceInstaller(), CacheServiceName.CONFIGURATION, ATTRIBUTES);
        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, CacheReadAttributeHandler.INSTANCE, restartWriteHandler);
        }
    }
}
