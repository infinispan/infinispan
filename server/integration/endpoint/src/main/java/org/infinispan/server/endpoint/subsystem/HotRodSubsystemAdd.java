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

import javax.security.sasl.Sasl;

import org.infinispan.server.endpoint.Constants;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.Property;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceName;


/**
 * @author Tristan Tarrant
 */
class HotRodSubsystemAdd extends ProtocolServiceSubsystemAdd {

   static final ProtocolServiceSubsystemAdd INSTANCE = new HotRodSubsystemAdd();

   @Override
   protected void populateModel(ModelNode source, ModelNode target) throws OperationFailedException {
      populate(source, target);
   }

   private static void populate(ModelNode source, ModelNode target) throws OperationFailedException {
      for(AttributeDefinition attr : ProtocolServerConnectorResource.COMMON_CONNECTOR_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
      for(AttributeDefinition attr : ProtocolServerConnectorResource.COMMON_LIST_CONNECTOR_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
      for(AttributeDefinition attr : ProtocolServerConnectorResource.PROTOCOL_SERVICE_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
   }

   @Override
   protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
      // Read the full model
      ModelNode config = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));
      // Create the builder
      HotRodServerConfigurationBuilder configurationBuilder = new HotRodServerConfigurationBuilder();
      configureProtocolServer(configurationBuilder, config);
      configureProtocolServerTopology(context, configurationBuilder, config);
      // Create the service
      final ProtocolServerService service = new ProtocolServerService(getServiceName(operation), HotRodServer.class, configurationBuilder);

      // Setup the various dependencies with injectors and install the service
      final ServiceName hotRodServerServiceName = EndpointUtils.getServiceName(operation, "hotrod");
      ServiceBuilder<?> builder = context.getServiceTarget().addService(hotRodServerServiceName, service);

      String cacheContainerName = getCacheContainerName(operation);
      EndpointUtils.addCacheContainerConfigurationDependency(builder, cacheContainerName, service.getCacheManagerConfiguration());
      EndpointUtils.addCacheContainerDependency(builder, cacheContainerName, service.getCacheManager());
      EndpointUtils.addCacheDependency(builder, cacheContainerName, null);
      EndpointUtils.addSocketBindingDependency(context, builder, getSocketBindingName(operation), service.getSocketBinding());

      EncryptableSubsystemHelper.processEncryption(context, config, service, builder);
      processAuthentication(context, configurationBuilder, service, builder, config);

      // Extension manager dependency
      builder.addDependency(Constants.EXTENSION_MANAGER_NAME, ExtensionManagerService.class, service.getExtensionManager());

      builder.install();
   }

   private void processAuthentication(OperationContext context, HotRodServerConfigurationBuilder configurationBuilder, ProtocolServerService service, ServiceBuilder<?> builder, ModelNode config) throws OperationFailedException {
      if (config.hasDefined(ModelKeys.AUTHENTICATION) && config.get(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME).isDefined()) {
         configurationBuilder.authentication().enable();
         ModelNode authentication = config.get(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME);

         EndpointUtils.addSecurityRealmDependency(builder, AuthenticationResource.SECURITY_REALM.resolveModelAttribute(context, authentication).asString(), service.getAuthenticationSecurityRealm());
         if (authentication.hasDefined(ModelKeys.SASL) && authentication.get(ModelKeys.SASL, ModelKeys.SASL_NAME).isDefined()) {
            AuthenticationConfigurationBuilder authenticationBuilder = configurationBuilder.authentication();
            ModelNode sasl = authentication.get(ModelKeys.SASL, ModelKeys.SASL_NAME);
            if (sasl.hasDefined(ModelKeys.SERVER_CONTEXT_NAME)) {
               String serverContextName = SaslResource.SERVER_CONTEXT_NAME.resolveModelAttribute(context, sasl).asString();
               service.setServerContextName(serverContextName);
               EndpointUtils.addSecurityDomainDependency(builder, serverContextName, service.getSaslSecurityDomain()); // FIXME: needed ???
            }

            if (sasl.hasDefined(ModelKeys.SERVER_NAME)) {
               authenticationBuilder.serverName(SaslResource.SERVER_NAME.resolveModelAttribute(context, sasl).asString());
            }
            if (sasl.hasDefined(ModelKeys.MECHANISMS)) {
               for(ModelNode mech : sasl.get(ModelKeys.MECHANISMS).asList()) {
                  authenticationBuilder.addAllowedMech(mech.asString());
               }
            }
            String qop = listAsString(sasl, ModelKeys.QOP);
            if (qop != null) {
               authenticationBuilder.addMechProperty(Sasl.QOP, qop);
            }
            String strength = listAsString(sasl, ModelKeys.STRENGTH);
            if (strength != null) {
               authenticationBuilder.addMechProperty(Sasl.STRENGTH, strength);
            }
            if (sasl.hasDefined(ModelKeys.SASL_POLICY) && sasl.get(ModelKeys.SASL_POLICY, ModelKeys.SASL_POLICY_NAME).isDefined()) {
               for(Property property : sasl.get(ModelKeys.SASL_POLICY, ModelKeys.SASL_POLICY_NAME).asPropertyList()) {
                  String value = property.getValue().asString();
                  switch (property.getName()) {
                     case ModelKeys.FORWARD_SECRECY:
                        authenticationBuilder.addMechProperty(Sasl.POLICY_FORWARD_SECRECY, value);
                        break;
                     case ModelKeys.NO_ACTIVE:
                        authenticationBuilder.addMechProperty(Sasl.POLICY_NOACTIVE, value);
                        break;
                     case ModelKeys.NO_ANONYMOUS:
                        authenticationBuilder.addMechProperty(Sasl.POLICY_NOANONYMOUS, value);
                        break;
                     case ModelKeys.NO_DICTIONARY:
                        authenticationBuilder.addMechProperty(Sasl.POLICY_NODICTIONARY, value);
                        break;
                     case ModelKeys.NO_PLAIN_TEXT:
                        authenticationBuilder.addMechProperty(Sasl.POLICY_NOPLAINTEXT, value);
                        break;
                     case ModelKeys.PASS_CREDENTIALS:
                        authenticationBuilder.addMechProperty(Sasl.POLICY_PASS_CREDENTIALS, value);
                        break;
                  }
               }
            }
            if (sasl.hasDefined(ModelKeys.PROPERTY)) {
               for(Property property : sasl.get(ModelKeys.PROPERTY).asPropertyList()) {
                  authenticationBuilder.addMechProperty(property.getName(), property.getValue().asProperty().getValue().asString());
               }
            }
         }
      }
   }

   private String listAsString(ModelNode node, String name) {
      if (node.hasDefined(name)) {
         StringBuilder sb = new StringBuilder();
         for(ModelNode item : node.get(name).asList()) {
            if (sb.length() > 0) {
               sb.append(' ');
            }
            sb.append(item.asString());
         }
         return sb.toString();
      } else {
         return null;
      }
   }

   private void configureProtocolServerTopology(OperationContext context, HotRodServerConfigurationBuilder builder, ModelNode config) throws OperationFailedException {
      if (config.hasDefined(ModelKeys.TOPOLOGY_STATE_TRANSFER) && config.get(ModelKeys.TOPOLOGY_STATE_TRANSFER, ModelKeys.TOPOLOGY_STATE_TRANSFER_NAME).isDefined()) {
         config = config.get(ModelKeys.TOPOLOGY_STATE_TRANSFER, ModelKeys.TOPOLOGY_STATE_TRANSFER_NAME);
         if (config.hasDefined(ModelKeys.LOCK_TIMEOUT)) {
            builder.topologyLockTimeout(TopologyStateTransferResource.LOCK_TIMEOUT.resolveModelAttribute(context, config).asLong());
         }
         if (config.hasDefined(ModelKeys.REPLICATION_TIMEOUT)) {
            builder.topologyReplTimeout(TopologyStateTransferResource.REPLICATION_TIMEOUT.resolveModelAttribute(context, config).asLong());
         }
         if (config.hasDefined(ModelKeys.EXTERNAL_HOST)) {
            builder.proxyHost(TopologyStateTransferResource.EXTERNAL_HOST.resolveModelAttribute(context, config).asString());
         }
         if (config.hasDefined(ModelKeys.EXTERNAL_PORT)) {
            builder.proxyPort(TopologyStateTransferResource.EXTERNAL_PORT.resolveModelAttribute(context, config).asInt());
         }
         if (config.hasDefined(ModelKeys.LAZY_RETRIEVAL)) {
            builder.topologyStateTransfer(!TopologyStateTransferResource.LAZY_RETRIEVAL.resolveModelAttribute(context, config).asBoolean());
         }
         if (config.hasDefined(ModelKeys.AWAIT_INITIAL_RETRIEVAL)) {
            builder.topologyAwaitInitialTransfer(TopologyStateTransferResource.AWAIT_INITIAL_RETRIEVAL.resolveModelAttribute(context, config).asBoolean());
         }
      }
   }
}
