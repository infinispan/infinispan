/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
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

import java.util.List;

import org.infinispan.server.websocket.WebSocketServer;
import org.infinispan.server.websocket.configuration.WebSocketServerConfigurationBuilder;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;

/**
 * WebSocketSubsystemAdd.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
class WebSocketSubsystemAdd extends ProtocolServiceSubsystemAdd {

   static final WebSocketSubsystemAdd INSTANCE = new WebSocketSubsystemAdd();

   private static void populate(ModelNode source, ModelNode target) throws OperationFailedException {
      target.setEmptyObject();

      for (AttributeDefinition attr : ProtocolServerConnectorResource.COMMON_CONNECTOR_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
      for (AttributeDefinition attr : ProtocolServerConnectorResource.PROTOCOL_SERVICE_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
   }

   @Override
   protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model, ServiceVerificationHandler verificationHandler,
         List<ServiceController<?>> newControllers) throws OperationFailedException {
      // Read the full model
      ModelNode config = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));
      // Create the builder
      WebSocketServerConfigurationBuilder configurationBuilder = new WebSocketServerConfigurationBuilder();
      this.configureProtocolServer(configurationBuilder, config);
      // Create the service
      final ProtocolServerService service = new ProtocolServerService(getServiceName(operation), WebSocketServer.class, configurationBuilder);

      // Setup the various dependencies with injectors and install the service
      ServiceBuilder<?> builder = context.getServiceTarget().addService(EndpointUtils.getServiceName(operation, "websocket"), service);
      EndpointUtils.addCacheContainerDependency(builder, getCacheContainerName(operation), service.getCacheManager());
      EndpointUtils.addSocketBindingDependency(builder, getSocketBindingName(operation), service.getSocketBinding());
      builder.install();
   }

   @Override
   protected void populateModel(ModelNode source, ModelNode target) throws OperationFailedException {
      populate(source, target);
   }

   @Override
   protected boolean requiresRuntimeVerification() {
      return false;
   }
}
