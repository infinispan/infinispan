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

import javax.security.sasl.Sasl;

import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.Property;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;

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
      for(AttributeDefinition attr : ProtocolServerConnectorResource.PROTOCOL_SERVICE_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
   }

   @Override
   protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model, ServiceVerificationHandler verificationHandler, List<ServiceController<?>> newControllers)
            throws OperationFailedException {
      // Read the full model
      ModelNode config = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));
      // Create the builder
      HotRodServerConfigurationBuilder configurationBuilder = new HotRodServerConfigurationBuilder();
      configureProtocolServer(configurationBuilder, config);
      configureProtocolServerAuthentication(configurationBuilder, config);
      configureProtocolServerEncryption(configurationBuilder, config);
      configureProtocolServerTopology(configurationBuilder, config);
      // Create the service
      final ProtocolServerService service = new ProtocolServerService(getServiceName(operation), HotRodServer.class, configurationBuilder);

      // Setup the various dependencies with injectors and install the service
      ServiceBuilder<?> builder = context.getServiceTarget().addService(EndpointUtils.getServiceName(operation, "hotrod"), service);

      String cacheContainerName = getCacheContainerName(operation);
      EndpointUtils.addCacheContainerConfigurationDependency(builder, cacheContainerName, service.getCacheManagerConfiguration());
      EndpointUtils.addCacheContainerDependency(builder, cacheContainerName, service.getCacheManager());
      EndpointUtils.addCacheDependency(builder, cacheContainerName, null);
      EndpointUtils.addSocketBindingDependency(builder, getSocketBindingName(operation), service.getSocketBinding());
      if (config.hasDefined(ModelKeys.ENCRYPTION) && config.get(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME).isDefined()) {
         EndpointUtils.addSecurityRealmDependency(builder, config.get(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME, ModelKeys.SECURITY_REALM).asString(), service.getEncryptionSecurityRealm());
      }
      if (config.hasDefined(ModelKeys.AUTHENTICATION) && config.get(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME).isDefined()) {
         ModelNode authentication = config.get(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME);
         EndpointUtils.addSecurityRealmDependency(builder, authentication.get(ModelKeys.SECURITY_REALM).asString(), service.getAuthenticationSecurityRealm());
         if (authentication.hasDefined(ModelKeys.SASL) && authentication.get(ModelKeys.SASL, ModelKeys.SASL_NAME).isDefined()) {
            AuthenticationConfigurationBuilder authenticationBuilder = configurationBuilder.authentication();
            ModelNode sasl = authentication.get(ModelKeys.SASL, ModelKeys.SASL_NAME);
            if (sasl.hasDefined(ModelKeys.SERVER_CONTEXT_NAME)) {
               String serverContextName = sasl.get(ModelKeys.SERVER_CONTEXT_NAME).asString();
               service.setServerContextName(serverContextName);
               EndpointUtils.addSecurityDomainDependency(builder, serverContextName, service.getSaslSecurityDomain()); // FIXME: needed ???
            }

            if (sasl.hasDefined(ModelKeys.SERVER_NAME)) {
               authenticationBuilder.serverName(sasl.get(ModelKeys.SERVER_NAME).asString());
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
            if (sasl.hasDefined(ModelKeys.PROPERTY)) {
               for(Property property : sasl.get(ModelKeys.PROPERTY).asPropertyList()) {
                  authenticationBuilder.addMechProperty(property.getName(), property.getValue().asProperty().getValue().asString());
               }
            }
         }
      }
      builder.install();
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

   private void configureProtocolServerTopology(HotRodServerConfigurationBuilder builder, ModelNode config) {
      if (config.hasDefined(ModelKeys.TOPOLOGY_STATE_TRANSFER) && config.get(ModelKeys.TOPOLOGY_STATE_TRANSFER, ModelKeys.TOPOLOGY_STATE_TRANSFER_NAME).isDefined()) {
         config = config.get(ModelKeys.TOPOLOGY_STATE_TRANSFER, ModelKeys.TOPOLOGY_STATE_TRANSFER_NAME);
         if (config.hasDefined(ModelKeys.LOCK_TIMEOUT)) {
            builder.topologyLockTimeout(config.get(ModelKeys.LOCK_TIMEOUT).asLong());
         }
         if (config.hasDefined(ModelKeys.REPLICATION_TIMEOUT)) {
            builder.topologyReplTimeout(config.get(ModelKeys.REPLICATION_TIMEOUT).asLong());
         }
         if (config.hasDefined(ModelKeys.EXTERNAL_HOST)) {
            builder.proxyHost(config.get(ModelKeys.EXTERNAL_HOST).asString());
         }
         if (config.hasDefined(ModelKeys.EXTERNAL_PORT)) {
            builder.proxyPort(config.get(ModelKeys.EXTERNAL_PORT).asInt());
         }
         if (config.hasDefined(ModelKeys.LAZY_RETRIEVAL)) {
            builder.topologyStateTransfer(!config.get(ModelKeys.LAZY_RETRIEVAL).asBoolean(false));
         }
         if (config.hasDefined(ModelKeys.AWAIT_INITIAL_RETRIEVAL)) {
            builder.topologyAwaitInitialTransfer(config.get(ModelKeys.AWAIT_INITIAL_RETRIEVAL).asBoolean());
         }
      }
   }

   private void configureProtocolServerAuthentication(HotRodServerConfigurationBuilder builder, ModelNode config) {
      if (config.hasDefined(ModelKeys.AUTHENTICATION) && config.get(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME).isDefined()) {
         config = config.get(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME);
         builder.authentication().enable();
      }
   }

   private void configureProtocolServerEncryption(HotRodServerConfigurationBuilder builder, ModelNode config) {
      if (config.hasDefined(ModelKeys.ENCRYPTION) && config.get(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME).isDefined()) {
         config = config.get(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME);
         builder.ssl().enable();
         if (config.hasDefined(ModelKeys.REQUIRE_SSL_CLIENT_AUTH)) {
            builder.ssl().requireClientAuth(config.get(ModelKeys.REQUIRE_SSL_CLIENT_AUTH).asBoolean());
         }
      }
   }

   @Override
   protected boolean requiresRuntimeVerification() {
      return false;
   }
}
