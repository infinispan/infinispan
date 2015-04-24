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

import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.EvictionType;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/eviction=EVICTION
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Tristan Tarrant
 */
public class EvictionResource extends CacheChildResource {

    public static final PathElement EVICTION_PATH = PathElement.pathElement(ModelKeys.EVICTION, ModelKeys.EVICTION_NAME);

    // attributes
    static final SimpleAttributeDefinition EVICTION_STRATEGY =
            new SimpleAttributeDefinitionBuilder(ModelKeys.STRATEGY, ModelType.STRING, true)
                    .setXmlName(Attribute.STRATEGY.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setValidator(new EnumValidator<>(EvictionStrategy.class, true, false))
                    .setDefaultValue(new ModelNode().set(EvictionStrategy.NONE.name()))
                    .build();

    static final SimpleAttributeDefinition MAX_ENTRIES =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_ENTRIES, ModelType.LONG, true)
                    .setXmlName(Attribute.MAX_ENTRIES.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(-1))
                    .build();

    static final SimpleAttributeDefinition SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.SIZE, ModelType.LONG, true)
                    .setXmlName(Attribute.SIZE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(-1))
                    .build();

    static final SimpleAttributeDefinition TYPE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.TYPE, ModelType.STRING, true)
                   .setXmlName(Attribute.TYPE.getLocalName())
                   .setAllowExpression(true)
                   .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                   .setValidator(new EnumValidator<>(EvictionType.class, true, false))
                   .setDefaultValue(new ModelNode().set(EvictionType.COUNT.name()))
                   .build();

    static final AttributeDefinition[] EVICTION_ATTRIBUTES = {EVICTION_STRATEGY, MAX_ENTRIES, SIZE, TYPE};

    public EvictionResource(CacheResource cacheResource) {
        super(EVICTION_PATH, ModelKeys.EVICTION, cacheResource, EVICTION_ATTRIBUTES);
    }
}
