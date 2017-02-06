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

import org.infinispan.configuration.cache.StateTransferConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/state-transfer=STATE_TRANSFER
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Tristan Tarrant
 */
public class StateTransferConfigurationResource extends CacheConfigurationChildResource {

    public static final PathElement PATH = PathElement.pathElement(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME);

    // attributes
    static final SimpleAttributeDefinition AWAIT_INITIAL_TRANSFER =
            new SimpleAttributeDefinitionBuilder(ModelKeys.AWAIT_INITIAL_TRANSFER, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.ENABLED.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(StateTransferConfiguration.AWAIT_INITIAL_TRANSFER.getDefaultValue()))
                    .build();

    static final SimpleAttributeDefinition CHUNK_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CHUNK_SIZE, ModelType.INT, true)
                    .setXmlName(Attribute.CHUNK_SIZE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(StateTransferConfiguration.CHUNK_SIZE.getDefaultValue()))
                    .build();

    // enabled (used in state transfer, rehashing)
    static final SimpleAttributeDefinition ENABLED =
            new SimpleAttributeDefinitionBuilder(ModelKeys.ENABLED, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.ENABLED.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(true))
                    .build();

    // timeout (used in state transfer, rehashing)
    static final SimpleAttributeDefinition TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.TIMEOUT, ModelType.LONG, true)
                    .setXmlName(Attribute.TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(StateTransferConfiguration.TIMEOUT.getDefaultValue()))
                    .build();

    static final AttributeDefinition[] ATTRIBUTES = {AWAIT_INITIAL_TRANSFER, ENABLED, TIMEOUT, CHUNK_SIZE};

    public StateTransferConfigurationResource(CacheConfigurationResource parent) {
        super(PATH, ModelKeys.STATE_TRANSFER, parent, ATTRIBUTES);
    }
}
