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

public class RouterConnectorResource extends SimpleResourceDefinition {

   public static final PathElement ROUTER_CONNECTOR_PATH = PathElement.pathElement(ModelKeys.ROUTER_CONNECTOR);

   static final SimpleAttributeDefinition HOTROD_SOCKET_BINDING =
           new SimpleAttributeDefinitionBuilder(ModelKeys.HOTROD_SOCKET_BINDING, ModelType.STRING, true)
                   .setAllowExpression(true)
                   .setXmlName(ModelKeys.HOTROD_SOCKET_BINDING)
                   .setRestartAllServices()
                   .build();

   static final SimpleAttributeDefinition REST_SOCKET_BINDING =
           new SimpleAttributeDefinitionBuilder(ModelKeys.REST_SOCKET_BINDING, ModelType.STRING, true)
                   .setAllowExpression(true)
                   .setXmlName(ModelKeys.REST_SOCKET_BINDING)
                   .setRestartAllServices()
                   .build();

   public final static SimpleAttributeDefinition SINGLE_PORT_SOCKET_BINDING =
         new SimpleAttributeDefinitionBuilder(ModelKeys.SINGLE_PORT_SOCKET_BINDING, ModelType.STRING, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.SINGLE_PORT_SOCKET_BINDING)
               .setRestartAllServices()
               .build();

   static final SimpleAttributeDefinition KEEP_ALIVE =
           new SimpleAttributeDefinitionBuilder(ModelKeys.KEEP_ALIVE, ModelType.BOOLEAN, true)
                   .setAllowExpression(true)
                   .setXmlName(ModelKeys.KEEP_ALIVE)
                   .setRestartAllServices()
                   .setDefaultValue(new ModelNode().set(false))
                   .build();

   static final SimpleAttributeDefinition TCP_NODELAY =
           new SimpleAttributeDefinitionBuilder(ModelKeys.TCP_NODELAY, ModelType.BOOLEAN, true)
                   .setAllowExpression(true)
                   .setXmlName(ModelKeys.TCP_NODELAY)
                   .setRestartAllServices()
                   .setDefaultValue(new ModelNode().set(true))
                   .build();

   static final SimpleAttributeDefinition RECEIVE_BUFFER_SIZE =
           new SimpleAttributeDefinitionBuilder(ModelKeys.RECEIVE_BUFFER_SIZE, ModelType.LONG, true)
                   .setAllowExpression(true)
                   .setXmlName(ModelKeys.RECEIVE_BUFFER_SIZE)
                   .setRestartAllServices()
                   .setDefaultValue(new ModelNode().set(0))
                   .build();

   static final SimpleAttributeDefinition SEND_BUFFER_SIZE =
           new SimpleAttributeDefinitionBuilder(ModelKeys.SEND_BUFFER_SIZE, ModelType.LONG, true)
                   .setAllowExpression(true)
                   .setXmlName(ModelKeys.SEND_BUFFER_SIZE)
                   .setRestartAllServices()
                   .setDefaultValue(new ModelNode().set(0))
                   .build();

   static final SimpleAttributeDefinition NAME =
           new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, true)
                   .setAllowExpression(true)
                   .setXmlName(ModelKeys.NAME)
                   .setRestartAllServices()
                   .build();

   static final SimpleAttributeDefinition[] ROUTER_CONNECTOR_ATTRIBUTES = { KEEP_ALIVE, TCP_NODELAY, SEND_BUFFER_SIZE, RECEIVE_BUFFER_SIZE, HOTROD_SOCKET_BINDING, REST_SOCKET_BINDING, SINGLE_PORT_SOCKET_BINDING, NAME };

   public RouterConnectorResource() {
      super(ROUTER_CONNECTOR_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.ROUTER_CONNECTOR), RouterSubsystemAdd.INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE);
   }

   @Override
   public void registerChildren(ManagementResourceRegistration resourceRegistration) {
      resourceRegistration.registerSubModel(new MultiTenancyResource());
      resourceRegistration.registerSubModel(new SinglePortResource());
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(ROUTER_CONNECTOR_ATTRIBUTES);
      for (AttributeDefinition attr : ROUTER_CONNECTOR_ATTRIBUTES) {
         resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }
   }

}
