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

import org.infinispan.server.router.configuration.builder.RouterConfigurationBuilder;
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

      RouterConfigurationBuilder configuration = new RouterConfigurationBuilder();
      RouterService routerService = new RouterService(configuration, getServiceName(config));

      final ServiceName routerServiceName = EndpointUtils.getServiceName(operation, "router");
      ServiceBuilder<?> builder = context.getServiceTarget().addService(routerServiceName, routerService);

      ModelNode multiTenancyInnerConfiguration = config.get(ModelKeys.MULTI_TENANCY, ModelKeys.MULTI_TENANCY_NAME);
      ModelNode SinglePortInnerConfiguration = config.get(ModelKeys.SINGLE_PORT, ModelKeys.SINGLE_PORT_NAME);

      addMultiTenantRest(context, operation, multiTenancyInnerConfiguration, routerService, builder);
      addMultiTenantHotRod(context, operation, multiTenancyInnerConfiguration, routerService, builder);

      addSinglePortHotRod(context, operation, SinglePortInnerConfiguration, routerService, builder);

      builder.install();
   }

   private void addSinglePortHotRod(OperationContext context, ModelNode operation, ModelNode config, RouterService routerService, ServiceBuilder<?> builder) throws OperationFailedException {
      if (config.isDefined()) {
         EndpointUtils.addSocketBindingDependency(context, builder, operation.get(ModelKeys.SINGLE_PORT_SOCKET_BINDING).asString(), routerService.getSinglePortSocketBinding());
         // We are parsing this model: {
         //    "security-realm" => "ClientCertRealm",
         //    "hotrod" => {"single-port-hotrod" => {"name" => "single-port-hotrod"}},
         //    "rest" => {"single-port-rest" => {"name" => "single-port-rest"}}
         //}
         RouterService.SinglePortRouting singlePortRouting = routerService.getSinglePortRouting();
         // We use endpoint names as keys in the model. That's very handy while parsing and writing to an XML
         // but it's causes a bit of headache when reading.
         ModelNode hotRodModelNode = config.get(ModelKeys.HOTROD).asList().get(0).get(0);
         ModelNode restModelNode = config.get(ModelKeys.REST).asList().get(0).get(0);
         ModelNode securityRealmModelNode = SinglePortResource.SECURITY_REALM.resolveModelAttribute(context, config);
         String securityRealm = securityRealmModelNode.asString();
         String hotRodServerName = SinglePortHotRodResource.NAME.resolveModelAttribute(context, hotRodModelNode).asString();
         String restServerName = SinglePortRestResource.NAME.resolveModelAttribute(context, restModelNode).asString();
         if (hotRodModelNode.isDefined()) {
            EndpointUtils.addHotRodDependency(builder, hotRodServerName, singlePortRouting.getHotrodServer());
         }
         if (restModelNode.isDefined()) {
            EndpointUtils.addRestDependency(builder, restServerName, singlePortRouting.getRestServer());
         }
         if (securityRealmModelNode.isDefined()) {
            EndpointUtils.addSecurityRealmDependency(builder, securityRealm, singlePortRouting.getSecurityRealm());
         }
      }
   }

   private void addMultiTenantRest(OperationContext context, ModelNode operation, ModelNode config, RouterService routerService, ServiceBuilder<?> builder) throws OperationFailedException {
      if (config.get(ModelKeys.REST).isDefined()) {
         EndpointUtils.addSocketBindingDependency(context, builder, operation.get(ModelKeys.REST_SOCKET_BINDING).asString(), routerService.getRestSocketBinding());
         for (ModelNode r : config.get(ModelKeys.REST).asList()) {
            ModelNode restNode = r.get(0);
            String restName = MultiTenantRestResource.NAME.resolveModelAttribute(context, restNode).asString();
            if (restNode.get(ModelKeys.PREFIX).isDefined()) {
               for (ModelNode prefixNode : restNode.get(ModelKeys.PREFIX).asList()) {
                  String pathPrefix = PrefixResource.PATH.resolveModelAttribute(context, prefixNode.get(0)).asString();
                  RouterService.RestRouting restRouting = routerService.getRestRouting(pathPrefix, restName);
                  EndpointUtils.addRestDependency(builder, restName, restRouting.getRest());
               }
            }
         }
      }
   }

   private void addMultiTenantHotRod(OperationContext context, ModelNode operation, ModelNode config, RouterService routerService, ServiceBuilder<?> builder) throws OperationFailedException {
      if(config.get(ModelKeys.HOTROD).isDefined()) {
         EndpointUtils.addSocketBindingDependency(context, builder, operation.get(ModelKeys.HOTROD_SOCKET_BINDING).asString(), routerService.getHotrodSocketBinding());
         for(ModelNode hr : config.get(ModelKeys.HOTROD).asList()) {
            ModelNode hotRod = hr.get(0);
            String hotRodName = MultiTenantHotRodResource.NAME.resolveModelAttribute(context, hotRod).asString();
            routerService.tcpNoDelay(RouterConnectorResource.TCP_NODELAY.resolveModelAttribute(context, hotRod).asBoolean());
            routerService.keepAlive(RouterConnectorResource.KEEP_ALIVE.resolveModelAttribute(context, hotRod).asBoolean());
            routerService.sendBufferSize(RouterConnectorResource.SEND_BUFFER_SIZE.resolveModelAttribute(context, hotRod).asInt());
            routerService.receiveBufferSize(RouterConnectorResource.RECEIVE_BUFFER_SIZE.resolveModelAttribute(context, hotRod).asInt());
            if(hotRod.get(ModelKeys.SNI).isDefined()) {
               for(ModelNode sni : hotRod.get(ModelKeys.SNI).asList()) {
                  ModelNode sniNode = sni.get(0);
                  String sniHostName = SniResource.HOST_NAME.resolveModelAttribute(context, sniNode).asString();
                  String securityRealm = SniResource.SECURITY_REALM.resolveModelAttribute(context, sniNode).asString();
                  RouterService.HotRodRouting hotRodRouting = routerService.getHotRodRouting(sniHostName);
                  EndpointUtils.addHotRodDependency(builder, hotRodName, hotRodRouting.getHotRod());
                  EndpointUtils.addSecurityRealmDependency(builder, securityRealm, hotRodRouting.getSecurityRealm());
               }
            }
         }
      }
   }

   private Optional<String> getServiceName(ModelNode config) {
      return Optional.ofNullable(config.get(ModelKeys.NAME)).map(ModelNode::asString);
   }

}
