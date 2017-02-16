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

import java.util.Optional;

import org.infinispan.server.router.configuration.builder.MultiTenantRouterConfigurationBuilder;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceName;

class RouterSubsystemAdd extends AbstractAddStepHandler {

   static final RouterSubsystemAdd INSTANCE = new RouterSubsystemAdd();

   @Override
   protected void populateModel(ModelNode source, ModelNode target) throws OperationFailedException {
      for(AttributeDefinition attr : RouterConnectorResource.ROUTER_CONNECTOR_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
   }

   @Override
   protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
      // Read the full model
      ModelNode config = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));

      MultiTenantRouterConfigurationBuilder configuration = new MultiTenantRouterConfigurationBuilder();
      MultiTenantRouterService routerService = new MultiTenantRouterService(configuration, getServiceName(config));

      final ServiceName MultitenantRouterServiceName = EndpointUtils.getServiceName(operation, "router");
      ServiceBuilder<?> builder = context.getServiceTarget().addService(MultitenantRouterServiceName, routerService);
      EndpointUtils.addSocketBindingDependency(builder, operation.get(ModelKeys.HOTROD_SOCKET_BINDING).asString(), routerService.getHotrodSocketBinding());
      EndpointUtils.addSocketBindingDependency(builder, operation.get(ModelKeys.REST_SOCKET_BINDING).asString(), routerService.getRestSocketBinding());

      ModelNode multiTenancyInnerConfiguration = config.get(ModelKeys.MULTI_TENANCY, ModelKeys.MULTI_TENANCY_NAME);

      addHotRod(multiTenancyInnerConfiguration, routerService, builder);
      addRest(multiTenancyInnerConfiguration, routerService, builder);

      builder.install();
   }

   private void addRest(ModelNode config, MultiTenantRouterService routerService, ServiceBuilder<?> builder) {
      if (config.get(ModelKeys.REST).isDefined()) {
         config.get(ModelKeys.REST).asList().forEach(r -> {
            String restName = r.get(0).get(ModelKeys.NAME).asString();
            if (r.get(0).get(ModelKeys.PREFIX).isDefined()) {
               r.get(0).get(ModelKeys.PREFIX).asList().forEach(prefix -> {
                  String pathPrefix = prefix.get(0).get(ModelKeys.PATH).asString();
                  MultiTenantRouterService.RestRouting restRouting = routerService.getRestRouting(pathPrefix, restName);
                  EndpointUtils.addRestDependency(builder, restName, restRouting.getRest());
               });
            }
         });
      }
   }

   private void addHotRod(ModelNode config, MultiTenantRouterService routerService, ServiceBuilder<?> builder) {
      if(config.get(ModelKeys.HOTROD).isDefined()) {
         config.get(ModelKeys.HOTROD).asList().forEach(hr -> {
            String hotRodName = hr.get(0).get(ModelKeys.NAME).asString();
            routerService.tcpNoDelay(hr.get(0).get(ModelKeys.TCP_NODELAY).asBoolean(true));
            routerService.keepAlive(hr.get(0).get(ModelKeys.KEEP_ALIVE).asBoolean(false));
            routerService.sendBufferSize(hr.get(0).get(ModelKeys.SEND_BUFFER_SIZE).asInt(0));
            routerService.receiveBufferSize(hr.get(0).get(ModelKeys.RECEIVE_BUFFER_SIZE).asInt(0));
            if(hr.get(0).get(ModelKeys.SNI).isDefined()) {
               hr.get(0).get(ModelKeys.SNI).asList().forEach(sni -> {
                  String sniHostName = sni.get(0).get(ModelKeys.HOST_NAME).asString();
                  String securityRealm = sni.get(0).get(ModelKeys.SECURITY_REALM).asString();
                  MultiTenantRouterService.HotRodRouting hotRodRouting = routerService.getHotRodRouting(sniHostName);
                  EndpointUtils.addHotRodDependency(builder, hotRodName, hotRodRouting.getHotRod());
                  EndpointUtils.addSecurityRealmDependency(builder, securityRealm, hotRodRouting.getSecurityRealm());
               });
            }
         });
      }
   }

   private Optional<String> getServiceName(ModelNode config) {
      return Optional.ofNullable(config.get(ModelKeys.NAME)).map(ModelNode::asString);
   }

}
