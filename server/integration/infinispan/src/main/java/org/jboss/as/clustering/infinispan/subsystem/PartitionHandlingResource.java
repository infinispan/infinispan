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
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/partition-handling=PARTITION_HANDLING
 *
 * @author Dan Berindei (c) 2014 Red Hat Inc.
 */
public class PartitionHandlingResource extends SimpleResourceDefinition {

    public static final PathElement PARTITION_HANDLING_PATH = PathElement.pathElement(ModelKeys.PARTITION_HANDLING, ModelKeys.PARTITION_HANDLING_NAME);

    // attributes
    static final SimpleAttributeDefinition ENABLED =
            new SimpleAttributeDefinitionBuilder(ModelKeys.ENABLED, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.ENABLED.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(true))
                    .build();

    static final AttributeDefinition[] PARTITION_HANDLING_ATTRIBUTES = {ENABLED};

    public PartitionHandlingResource() {
        super(PARTITION_HANDLING_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.PARTITION_HANDLING),
                CacheConfigOperationHandlers.PARTITION_HANDLING_ADD,
                ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // check that we don't need a special handler here?
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(PARTITION_HANDLING_ATTRIBUTES);
        for (AttributeDefinition attr : PARTITION_HANDLING_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
    }
}
