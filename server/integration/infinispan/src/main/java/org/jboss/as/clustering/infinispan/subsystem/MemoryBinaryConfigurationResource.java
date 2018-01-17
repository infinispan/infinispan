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

import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/memory=MEMORY-BINARY
 *
 * @author William Burns
 */
public class MemoryBinaryConfigurationResource extends CacheConfigurationChildResource {

    public static final PathElement PATH = PathElement.pathElement(ModelKeys.MEMORY, ModelKeys.BINARY_NAME);

    // attributes
    static final SimpleAttributeDefinition SIZE =
          new SimpleAttributeDefinitionBuilder(ModelKeys.SIZE, ModelType.LONG, true)
                .setXmlName(Attribute.SIZE.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_NONE)
                .setDefaultValue(new ModelNode().set(MemoryConfiguration.SIZE.getDefaultValue()))
                .build();

    static final SimpleAttributeDefinition EVICTION =
          new SimpleAttributeDefinitionBuilder(ModelKeys.EVICTION, ModelType.STRING, true)
                .setXmlName(Attribute.TYPE.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setValidator(new EnumValidator<>(EvictionType.class, true, false))
                .setDefaultValue(new ModelNode().set(MemoryConfiguration.EVICTION_TYPE.getDefaultValue().name()))
                .build();

    static final SimpleAttributeDefinition STRATEGY =
          new SimpleAttributeDefinitionBuilder(ModelKeys.STRATEGY, ModelType.STRING, true)
                .setXmlName(Attribute.STRATEGY.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setValidator(new EnumValidator<>(EvictionStrategy.class, true, false))
                .setDefaultValue(new ModelNode().set(EvictionStrategy.NONE.name()))
                .build();

    static final AttributeDefinition[] ATTRIBUTES = {SIZE, EVICTION, STRATEGY};

    public MemoryBinaryConfigurationResource(CacheConfigurationResource parent) {
        super(PATH, ModelKeys.MEMORY, parent, ATTRIBUTES);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        final OperationStepHandler restartCacheWriteHandler = new RestartServiceWriteAttributeHandler(
              resource.getPathElement().getKey(), resource.getServiceInstaller(), CacheServiceName.CONFIGURATION, attributes);

        resourceRegistration.registerReadWriteAttribute(EVICTION, CacheReadAttributeHandler.INSTANCE, restartCacheWriteHandler);
        resourceRegistration.registerReadWriteAttribute(STRATEGY, CacheReadAttributeHandler.INSTANCE, restartCacheWriteHandler);
        resourceRegistration.registerReadWriteAttribute(SIZE, CacheReadAttributeHandler.INSTANCE, new RuntimeCacheConfigurationWriteAttributeHandler(SIZE, (configuration, newSize) -> {
            configuration.memory().size(newSize.asLong());
        }));
    }
}
