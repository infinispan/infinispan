/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.endpoint.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

public class TopologyStateTransferResource extends SimpleResourceDefinition {
   private static final PathElement TOPOLOGY_PATH = PathElement.pathElement(ModelKeys.TOPOLOGY_STATE_TRANSFER, ModelKeys.TOPOLOGY_STATE_TRANSFER_NAME);

   static final SimpleAttributeDefinition EXTERNAL_HOST =
         new SimpleAttributeDefinitionBuilder(ModelKeys.EXTERNAL_HOST, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.EXTERNAL_HOST)
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition EXTERNAL_PORT =
         new SimpleAttributeDefinitionBuilder(ModelKeys.EXTERNAL_PORT, ModelType.INT, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.EXTERNAL_PORT)
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition LAZY_RETRIEVAL =
         new SimpleAttributeDefinitionBuilder(ModelKeys.LAZY_RETRIEVAL, ModelType.BOOLEAN, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.LAZY_RETRIEVAL)
                 .setRestartAllServices()
                 .setDefaultValue(new ModelNode().set(false))
                 .build();

   static final SimpleAttributeDefinition AWAIT_INITIAL_RETRIEVAL =
         new SimpleAttributeDefinitionBuilder(ModelKeys.AWAIT_INITIAL_RETRIEVAL, ModelType.BOOLEAN, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.AWAIT_INITIAL_RETRIEVAL)
                 .setRestartAllServices()
                 .setDefaultValue(new ModelNode().set(false))
                 .build();

   static final SimpleAttributeDefinition LOCK_TIMEOUT =
         new SimpleAttributeDefinitionBuilder(ModelKeys.LOCK_TIMEOUT, ModelType.LONG, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.LOCK_TIMEOUT)
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition REPLICATION_TIMEOUT =
         new SimpleAttributeDefinitionBuilder(ModelKeys.REPLICATION_TIMEOUT, ModelType.LONG, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.REPLICATION_TIMEOUT)
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition[] TOPOLOGY_ATTRIBUTES = { EXTERNAL_HOST, EXTERNAL_PORT, LAZY_RETRIEVAL, AWAIT_INITIAL_RETRIEVAL, LOCK_TIMEOUT, REPLICATION_TIMEOUT };

   public TopologyStateTransferResource() {
      super(TOPOLOGY_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.TOPOLOGY_STATE_TRANSFER), TopologyStateTransferAdd.INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE);
  }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
       super.registerAttributes(resourceRegistration);

       final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(TOPOLOGY_ATTRIBUTES);
       for (AttributeDefinition attr : TOPOLOGY_ATTRIBUTES) {
           resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
       }
   }
}
