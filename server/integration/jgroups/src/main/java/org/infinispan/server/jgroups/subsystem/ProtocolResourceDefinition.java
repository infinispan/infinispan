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

package org.infinispan.server.jgroups.subsystem;

import org.infinispan.server.commons.controller.AttributeMarshallers;
import org.infinispan.server.commons.controller.validation.ModuleIdentifierValidator;
import org.infinispan.server.jgroups.spi.ProtocolConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleMapAttributeDefinition;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.access.management.SensitiveTargetAccessConstraintDefinition;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for /subsystem=jgroups/stack=X/protocol=Y
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class ProtocolResourceDefinition extends SimpleResourceDefinition {

    static final PathElement WILDCARD_PATH = pathElement(PathElement.WILDCARD_VALUE);

    static PathElement pathElement(String name) {
        return PathElement.pathElement(ModelKeys.PROTOCOL, name);
    }

    @Deprecated
    static final SimpleAttributeDefinition TYPE = new SimpleAttributeDefinitionBuilder(ModelKeys.TYPE, ModelType.STRING, true)
            .setXmlName(Attribute.TYPE.getLocalName())
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .setDeprecated(JGroupsModel.VERSION_3_0_0.getVersion())
            .build();

    static final SimpleAttributeDefinition SOCKET_BINDING = new SimpleAttributeDefinitionBuilder(ModelKeys.SOCKET_BINDING, ModelType.STRING, true)
            .setXmlName(Attribute.SOCKET_BINDING.getLocalName())
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_BINDING_REF)
            .build();

    static final SimpleAttributeDefinition MODULE = new SimpleAttributeDefinitionBuilder(ModelKeys.MODULE, ModelType.STRING, true)
            .setXmlName(Attribute.MODULE.getLocalName())
            .setDefaultValue(new ModelNode(ProtocolConfiguration.DEFAULT_MODULE.getName()))
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .setValidator(new ModuleIdentifierValidator(true))
            .build();

    static final SimpleMapAttributeDefinition PROPERTIES = new SimpleMapAttributeDefinition.Builder("properties", true)
            .setAllowExpression(true)
            .setAttributeMarshaller(AttributeMarshallers.PROPERTY_LIST)
            .setDefaultValue(new ModelNode().setEmptyList())
            .build();

    static final AttributeDefinition[] ATTRIBUTES = new AttributeDefinition[] { TYPE, MODULE, SOCKET_BINDING, PROPERTIES };

    ProtocolResourceDefinition() {
        this(new ReloadRequiredAddStepHandler(ATTRIBUTES));
    }

    ProtocolResourceDefinition(OperationStepHandler addHandler) {
        super(WILDCARD_PATH, new JGroupsResourceDescriptionResolver(ModelKeys.PROTOCOL), addHandler, new ReloadRequiredRemoveStepHandler());
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration registration) {
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(ATTRIBUTES);
        for (AttributeDefinition attr : ATTRIBUTES) {
            registration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }

    @Override
    public void registerChildren(ManagementResourceRegistration registration) {
        registration.registerSubModel(new PropertyResourceDefinition());
    }
}
