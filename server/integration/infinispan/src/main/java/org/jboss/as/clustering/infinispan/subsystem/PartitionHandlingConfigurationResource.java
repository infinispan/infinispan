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

import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.configuration.parsing.Parser;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/partition-handling=PARTITION_HANDLING
 *
 * @author Dan Berindei (c) 2014 Red Hat Inc.
 * @author Tristan Tarrant
 */
public class PartitionHandlingConfigurationResource extends CacheConfigurationChildResource {

    public static final PathElement PATH = PathElement.pathElement(ModelKeys.PARTITION_HANDLING, ModelKeys.PARTITION_HANDLING_NAME);

    // attributes
    @Deprecated
    static final SimpleAttributeDefinition ENABLED =
            new SimpleAttributeDefinitionBuilder(ModelKeys.ENABLED, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.ENABLED.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(true))
                    .setDeprecated(Namespace.INFINISPAN_SERVER_9_1.getVersion())
                    .build();

    static final SimpleAttributeDefinition WHEN_SPLIT =
          new SimpleAttributeDefinitionBuilder(ModelKeys.WHEN_SPLIT, ModelType.STRING, true)
                .setXmlName(Attribute.WHEN_SPLIT.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setDefaultValue(new ModelNode().set(PartitionHandlingConfiguration.WHEN_SPLIT.getDefaultValue().name()))
                .build();

    static final SimpleAttributeDefinition MERGE_POLICY =
          new SimpleAttributeDefinitionBuilder(ModelKeys.MERGE_POLICY, ModelType.STRING, true)
                .setXmlName(Attribute.ENABLED.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setDefaultValue(new ModelNode().set(Parser.MergePolicy.NONE.toString()))
                .build();

    static final AttributeDefinition[] ATTRIBUTES = {ENABLED, WHEN_SPLIT, MERGE_POLICY};

    PartitionHandlingConfigurationResource(CacheConfigurationResource parent) {
        super(PATH, ModelKeys.PARTITION_HANDLING, parent, ATTRIBUTES);
    }
}
