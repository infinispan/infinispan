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

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/transport=TRANSPORT
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class TransportResource extends SimpleResourceDefinition {

    public static final PathElement TRANSPORT_PATH = PathElement.pathElement(ModelKeys.TRANSPORT, ModelKeys.TRANSPORT_NAME);

    // attributes
    static final SimpleAttributeDefinition CHANNEL =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CHANNEL, ModelType.STRING, true)
                    .setXmlName(Attribute.CHANNEL.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition INITIAL_CLUSTER_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.INITIAL_CLUSTER_SIZE, ModelType.INT, true)
                    .setXmlName(Attribute.INITIAL_CLUSTER_SIZE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition INITIAL_CLUSTER_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.INITIAL_CLUSTER_TIMEOUT, ModelType.LONG, true)
                    .setXmlName(Attribute.INITIAL_CLUSTER_TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition LOCK_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.LOCK_TIMEOUT, ModelType.LONG, true)
                    .setXmlName(Attribute.LOCK_TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(240000))
                    .build();

    static final SimpleAttributeDefinition STRICT_PEER_TO_PEER =
            new SimpleAttributeDefinitionBuilder(ModelKeys.STRICT_PEER_TO_PEER, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.STRICT_PEER_TO_PEER.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();

    static final AttributeDefinition[] TRANSPORT_ATTRIBUTES = {CHANNEL, INITIAL_CLUSTER_SIZE, INITIAL_CLUSTER_TIMEOUT, LOCK_TIMEOUT, STRICT_PEER_TO_PEER};

    public TransportResource() {
        super(TRANSPORT_PATH,
                new InfinispanResourceDescriptionResolver(ModelKeys.TRANSPORT),
                new ReloadRequiredAddStepHandler(TRANSPORT_ATTRIBUTES),
                ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(TRANSPORT_ATTRIBUTES);
        for (AttributeDefinition attr : TRANSPORT_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }
}
