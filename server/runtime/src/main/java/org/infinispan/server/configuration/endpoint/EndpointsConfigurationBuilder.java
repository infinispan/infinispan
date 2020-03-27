package org.infinispan.server.configuration.endpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

/**
 * @since 10.0
 */
public class EndpointsConfigurationBuilder implements Builder<EndpointsConfiguration> {
   private final AttributeSet attributes;
   private final ServerConfigurationBuilder serverConfigurationBuilder;
   private final List<ProtocolServerConfigurationBuilder<?, ?>> connectorBuilders = new ArrayList<>(2);
   private final SinglePortServerConfigurationBuilder singlePortBuilder = new SinglePortServerConfigurationBuilder();

   public EndpointsConfigurationBuilder(ServerConfigurationBuilder serverConfigurationBuilder) {
      this.serverConfigurationBuilder = serverConfigurationBuilder;
      this.attributes = EndpointsConfiguration.attributeDefinitionSet();
   }

   public EndpointsConfigurationBuilder socketBinding(String name) {
      attributes.attribute(EndpointsConfiguration.SOCKET_BINDING).set(name);
      serverConfigurationBuilder.applySocketBinding(name, singlePortBuilder);
      return this;
   }

   public EndpointsConfigurationBuilder securityRealm(String name) {
      attributes.attribute(EndpointsConfiguration.SECURITY_REALM).set(name);
      singlePortBuilder.securityRealm(serverConfigurationBuilder.getSecurityRealm(name));
      return this;
   }

   public EndpointsConfigurationBuilder implicitConnectorSecurity(boolean implicitConnectorSecurity) {
      attributes.attribute(EndpointsConfiguration.IMPLICIT_CONNECTOR_SECURITY).set(implicitConnectorSecurity);
      return this;
   }

   public List<ProtocolServerConfigurationBuilder<?, ?>> connectors() {
      return connectorBuilders;
   }

   public SinglePortServerConfigurationBuilder singlePort() {
      return singlePortBuilder;
   }

   public <T extends ProtocolServerConfigurationBuilder<?, ?>> T addConnector(Class<T> klass) {
      try {
         T builder = klass.getConstructor().newInstance();
         connectorBuilders.add(builder);
         singlePortBuilder.applyConfigurationToProtocol(builder);
         return builder;
      } catch (Exception e) {
         throw Server.log.cannotInstantiateProtocolServerConfigurationBuilder(klass, e);
      }
   }

   @Override
   public void validate() {
      Map<Class<?>, List<ProtocolServerConfigurationBuilder<?, ?>>> buildersPerClass = connectorBuilders.stream()
            .collect(Collectors.groupingBy(ProtocolServerConfigurationBuilder::getClass));

      Optional<Entry<Class<?>, List<ProtocolServerConfigurationBuilder<?, ?>>>> repeated = buildersPerClass.entrySet()
            .stream().filter(e -> e.getValue().size() > 1).findFirst();

      repeated.ifPresent(e -> {
         String names = e.getValue().stream().map(ProtocolServerConfigurationBuilder::name).collect(Collectors.joining(","));
         throw Server.log.multipleEndpointsSameTypeFound(names);
      });
   }

   @Override
   public EndpointsConfiguration create() {
      boolean implicitSecurity = attributes.attribute(EndpointsConfiguration.IMPLICIT_CONNECTOR_SECURITY).get() && singlePortBuilder.securityRealm() != null;
      List<ProtocolServerConfiguration> connectors = new ArrayList<>(connectorBuilders.size());
      for (ProtocolServerConfigurationBuilder<?, ?> builder : connectorBuilders) {
         if (implicitSecurity) {
            if (builder instanceof HotRodServerConfigurationBuilder) {
               enableImplicitAuthentication(singlePortBuilder.securityRealm(), (HotRodServerConfigurationBuilder) builder);
            } else if (builder instanceof RestServerConfigurationBuilder) {
               enableImplicitAuthentication(singlePortBuilder.securityRealm(), (RestServerConfigurationBuilder) builder);
            }
         }
         connectors.add(builder.create());
      }
      return new EndpointsConfiguration(attributes.protect(), connectors, singlePortBuilder.create());
   }

   @Override
   public EndpointsConfigurationBuilder read(EndpointsConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   private void enableImplicitAuthentication(ServerSecurityRealm securityRealm, HotRodServerConfigurationBuilder builder) {
      builder.authentication().enable().securityRealm(securityRealm.getName());
      String serverPrincipal = null;
      for (KerberosSecurityFactoryConfiguration identity : securityRealm.getServerIdentities().kerberosConfigurations()) {
         if (identity.getPrincipal().startsWith("hotrod/")) {
            builder.authentication()
                  .addMechanisms(SaslMechanismInformation.Names.GS2_KRB5, SaslMechanismInformation.Names.GSSAPI);
            serverPrincipal = identity.getPrincipal();
            break;
         }
         Server.log.debugf("Enabled Kerberos mechanisms for Hot Rod using principal '%s'", identity.getPrincipal());
      }
      if (securityRealm.hasFeature(ServerSecurityRealm.Feature.TOKEN)) {
         builder.authentication().addMechanisms(SaslMechanismInformation.Names.OAUTHBEARER);
         Server.log.debug("Enabled OAUTHBEARER mechanism for Hot Rod");
      }
      if (securityRealm.hasFeature(ServerSecurityRealm.Feature.TRUST)) {
         builder.authentication().addMechanisms(SaslMechanismInformation.Names.EXTERNAL);
         Server.log.debug("Enabled EXTERNAL mechanism for Hot Rod");
      }
      if (securityRealm.hasFeature(ServerSecurityRealm.Feature.PASSWORD)) {
         builder.authentication()
               .addMechanisms(
                     SaslMechanismInformation.Names.SCRAM_SHA_512,
                     SaslMechanismInformation.Names.SCRAM_SHA_384,
                     SaslMechanismInformation.Names.SCRAM_SHA_256,
                     SaslMechanismInformation.Names.SCRAM_SHA_1,
                     SaslMechanismInformation.Names.DIGEST_SHA_512,
                     SaslMechanismInformation.Names.DIGEST_SHA_384,
                     SaslMechanismInformation.Names.DIGEST_SHA_256,
                     SaslMechanismInformation.Names.DIGEST_SHA,
                     SaslMechanismInformation.Names.CRAM_MD5,
                     SaslMechanismInformation.Names.DIGEST_MD5
               );
         Server.log.debug("Enabled SCRAM, DIGEST and CRAM mechanisms for Hot Rod");
      }
      // Only enable PLAIN if encryption is on
      if (singlePortBuilder.ssl().isEnabled()) {
         builder.authentication().addMechanisms(SaslMechanismInformation.Names.PLAIN);
         Server.log.debug("Enabled PLAIN mechanism for Hot Rod");
      }
      builder.authentication().serverAuthenticationProvider(securityRealm.getSASLAuthenticationProvider(serverPrincipal));
   }

   private void enableImplicitAuthentication(ServerSecurityRealm securityRealm, RestServerConfigurationBuilder builder) {
      builder.authentication().enable().securityRealm(securityRealm.getName());
      String serverPrincipal = null;
      for (KerberosSecurityFactoryConfiguration identity : securityRealm.getServerIdentities().kerberosConfigurations()) {
         if (identity.getPrincipal().startsWith("HTTP/")) {
            builder.authentication().addMechanisms("SPNEGO");
            serverPrincipal = identity.getPrincipal();
         }
         Server.log.debugf("Enabled SPNEGO authentication for HTTP using principal '%s'", identity.getPrincipal());
      }
      if (securityRealm.hasFeature(ServerSecurityRealm.Feature.TOKEN)) {
         builder.authentication().addMechanisms("BEARER_TOKEN");
         Server.log.debug("Enabled BEARER_TOKEN for HTTP");
      }
      if (securityRealm.hasFeature(ServerSecurityRealm.Feature.TRUST)) {
         builder.authentication().addMechanisms("CLIENT_CERT");
         Server.log.debug("Enabled CLIENT_CERT for HTTP");
      }
      if (securityRealm.hasFeature(ServerSecurityRealm.Feature.PASSWORD)) {
         builder.authentication().addMechanisms("DIGEST");
         Server.log.debug("Enabled DIGEST for HTTP");
      }
      // Only enable PLAIN if encryption is on
      if (singlePortBuilder.ssl().isEnabled()) {
         builder.authentication().addMechanisms("BASIC");
         Server.log.debug("Enabled BASIC for HTTP");
      }
      builder.authentication().authenticator(securityRealm.getHTTPAuthenticationProvider(serverPrincipal));
   }
}
