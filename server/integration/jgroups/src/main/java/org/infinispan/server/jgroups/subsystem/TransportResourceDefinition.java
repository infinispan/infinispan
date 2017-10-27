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

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.access.management.SensitiveTargetAccessConstraintDefinition;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for /subsystem=jgroups/stack=X/transport=*
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class TransportResourceDefinition extends SimpleResourceDefinition {

    static final PathElement WILDCARD_PATH = pathElement(PathElement.WILDCARD_VALUE);

    static PathElement pathElement(String name) {
        return PathElement.pathElement(ModelKeys.TRANSPORT, name);
    }

    static final SimpleAttributeDefinition SHARED = new SimpleAttributeDefinitionBuilder(ModelKeys.SHARED, ModelType.BOOLEAN, true)
            .setXmlName(Attribute.SHARED.getLocalName())
            .setAllowExpression(true)
            .setDefaultValue(new ModelNode().set(false))
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .build();

    static final SimpleAttributeDefinition DIAGNOSTICS_SOCKET_BINDING = new SimpleAttributeDefinitionBuilder(ModelKeys.DIAGNOSTICS_SOCKET_BINDING, ModelType.STRING, true)
            .setXmlName(Attribute.DIAGNOSTICS_SOCKET_BINDING.getLocalName())
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .addAccessConstraint(SensitiveTargetAccessConstraintDefinition.SOCKET_BINDING_REF)
            .build();

    @Deprecated
    static final SimpleAttributeDefinition DEFAULT_EXECUTOR = new SimpleAttributeDefinitionBuilder(ModelKeys.DEFAULT_EXECUTOR, ModelType.STRING, true)
            .setDeprecated(JGroupsModel.VERSION_3_0_0.getVersion())
            .setXmlName(Attribute.DEFAULT_EXECUTOR.getLocalName())
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .build();

    @Deprecated
    static final SimpleAttributeDefinition OOB_EXECUTOR = new SimpleAttributeDefinitionBuilder(ModelKeys.OOB_EXECUTOR, ModelType.STRING, true)
            .setDeprecated(JGroupsModel.VERSION_3_0_0.getVersion())
            .setXmlName(Attribute.OOB_EXECUTOR.getLocalName())
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .build();

    @Deprecated
    static final SimpleAttributeDefinition TIMER_EXECUTOR = new SimpleAttributeDefinitionBuilder(ModelKeys.TIMER_EXECUTOR, ModelType.STRING, true)
            .setDeprecated(JGroupsModel.VERSION_3_0_0.getVersion())
            .setXmlName(Attribute.TIMER_EXECUTOR.getLocalName())
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .build();

    @Deprecated
    static final SimpleAttributeDefinition THREAD_FACTORY = new SimpleAttributeDefinitionBuilder(ModelKeys.THREAD_FACTORY, ModelType.STRING, true)
            .setDeprecated(JGroupsModel.VERSION_3_0_0.getVersion())
            .setXmlName(Attribute.THREAD_FACTORY.getLocalName())
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .build();

    static final SimpleAttributeDefinition SITE = new SimpleAttributeDefinitionBuilder(ModelKeys.SITE, ModelType.STRING, true)
            .setXmlName(Attribute.SITE.getLocalName())
            .setAllowExpression(true)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .build();

    static final SimpleAttributeDefinition RACK = new SimpleAttributeDefinitionBuilder(ModelKeys.RACK, ModelType.STRING, true)
            .setXmlName(Attribute.RACK.getLocalName())
            .setAllowExpression(true)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .build();

    static final SimpleAttributeDefinition MACHINE = new SimpleAttributeDefinitionBuilder(ModelKeys.MACHINE, ModelType.STRING, true)
            .setXmlName(Attribute.MACHINE.getLocalName())
            .setAllowExpression(true)
            .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
            .build();

    // the list of attributes used by the transport resource
    static final AttributeDefinition[] ATTRIBUTES = new AttributeDefinition[] {
            ProtocolResourceDefinition.TYPE, ProtocolResourceDefinition.MODULE, SHARED, ProtocolResourceDefinition.SOCKET_BINDING, DIAGNOSTICS_SOCKET_BINDING,
            ProtocolResourceDefinition.PROPERTIES, DEFAULT_EXECUTOR, OOB_EXECUTOR, TIMER_EXECUTOR, THREAD_FACTORY, SITE, RACK, MACHINE
    };

    TransportResourceDefinition() {
        super(WILDCARD_PATH, new JGroupsResourceDescriptionResolver(ModelKeys.TRANSPORT), new ReloadRequiredAddStepHandler(ATTRIBUTES), ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration registration) {
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(ATTRIBUTES);
        for (AttributeDefinition attr : ATTRIBUTES) {
            if (attr.isDeprecated()) {
                registration.registerReadWriteAttribute(attr, null, new ThreadsAttributesWriteHandler(attr));
            } else {
                registration.registerReadWriteAttribute(attr, null, writeHandler);
            }
        }
    }

    @Override
    public void registerChildren(ManagementResourceRegistration registration) {
        for (ThreadPoolResourceDefinition pool : ThreadPoolResourceDefinition.values()) {
            registration.registerSubModel(pool);
        }
    }
}
