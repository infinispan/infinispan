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

import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.endpoint.Constants;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.capability.RuntimeCapability;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

public class ProtocolServerConnectorResource extends CommonConnectorResource {

   static final String SOCKET_CAPABILITY_NAME = "org.wildfly.network.socket-binding";
   private static final RuntimeCapability<Void> CONNECTOR_CAPABILITY =
         RuntimeCapability.Builder.of(Constants.DATAGRID.getCanonicalName(), true).build();

   static final SimpleAttributeDefinition SOCKET_BINDING =
         new SimpleAttributeDefinitionBuilder(ModelKeys.SOCKET_BINDING, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.SOCKET_BINDING)
                 .setCapabilityReference(SOCKET_CAPABILITY_NAME, CONNECTOR_CAPABILITY)
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition IO_THREADS =
         new SimpleAttributeDefinitionBuilder(ModelKeys.IO_THREADS, ModelType.INT, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.IO_THREADS)
               .setDefaultValue(new ModelNode().set(ProtocolServerConfiguration.IO_THREADS.getDefaultValue()))
               .setRestartAllServices()
               .build();

   static final SimpleAttributeDefinition WORKER_THREADS =
         new SimpleAttributeDefinitionBuilder(ModelKeys.WORKER_THREADS, ModelType.INT, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.WORKER_THREADS)
                 .setDefaultValue(new ModelNode().set(ProtocolServerConfiguration.WORKER_THREADS.getDefaultValue()))
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition IDLE_TIMEOUT =
         new SimpleAttributeDefinitionBuilder(ModelKeys.IDLE_TIMEOUT, ModelType.INT, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.IDLE_TIMEOUT)
                 .setDefaultValue(new ModelNode().set(ProtocolServerConfiguration.IDLE_TIMEOUT.getDefaultValue()))
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition TCP_NODELAY =
         new SimpleAttributeDefinitionBuilder(ModelKeys.TCP_NODELAY, ModelType.BOOLEAN, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.TCP_NODELAY)
                 .setRestartAllServices()
                 .setDefaultValue(new ModelNode().set(ProtocolServerConfiguration.TCP_NODELAY.getDefaultValue()))
                 .build();

   static final SimpleAttributeDefinition TCP_KEEPALIVE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.TCP_KEEPALIVE, ModelType.BOOLEAN, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.TCP_KEEPALIVE)
               .setRestartAllServices()
               .setDefaultValue(new ModelNode().set(ProtocolServerConfiguration.TCP_KEEPALIVE.getDefaultValue()))
               .build();

   static final SimpleAttributeDefinition RECEIVE_BUFFER_SIZE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.RECEIVE_BUFFER_SIZE, ModelType.LONG, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.RECEIVE_BUFFER_SIZE)
                 .setRestartAllServices()
                 .setDefaultValue(new ModelNode().set(ProtocolServerConfiguration.RECV_BUF_SIZE.getDefaultValue()))
                 .build();

   static final SimpleAttributeDefinition SEND_BUFFER_SIZE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.SEND_BUFFER_SIZE, ModelType.LONG, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.SEND_BUFFER_SIZE)
                 .setRestartAllServices()
                 .setDefaultValue(new ModelNode().set(ProtocolServerConfiguration.SEND_BUF_SIZE.getDefaultValue()))
                 .build();

   static final SimpleAttributeDefinition[] PROTOCOL_SERVICE_ATTRIBUTES = {
         SOCKET_BINDING, IDLE_TIMEOUT, TCP_NODELAY, TCP_KEEPALIVE, RECEIVE_BUFFER_SIZE, SEND_BUFFER_SIZE, IO_THREADS, WORKER_THREADS
   };



   public ProtocolServerConnectorResource(PathElement pathElement, ResourceDescriptionResolver descriptionResolver, OperationStepHandler addHandler, OperationStepHandler removeHandler, boolean runtimeRegistration) {
      super(pathElement, descriptionResolver, addHandler, removeHandler, runtimeRegistration);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      super.registerAttributes(resourceRegistration);

      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(PROTOCOL_SERVICE_ATTRIBUTES);
      for (AttributeDefinition attr : PROTOCOL_SERVICE_ATTRIBUTES) {
         resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }
   }

   @Override
   public void registerCapabilities(ManagementResourceRegistration resourceRegistration) {
      super.registerCapabilities(resourceRegistration);
      resourceRegistration.registerCapability(CONNECTOR_CAPABILITY);
   }
}
