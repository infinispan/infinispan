package org.infinispan.server.configuration.endpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

/**
 * @since 12.0
 */
public class EndpointConfigurationBuilder implements Builder<EndpointConfiguration> {
   private final AttributeSet attributes;
   private final ServerConfigurationBuilder serverConfigurationBuilder;
   private final List<ProtocolServerConfigurationBuilder<?, ?>> connectorBuilders = new ArrayList<>(2);
   private final SinglePortServerConfigurationBuilder singlePortBuilder = new SinglePortServerConfigurationBuilder();
   private boolean implicitConnectorSecurity;

   public EndpointConfigurationBuilder(ServerConfigurationBuilder serverConfigurationBuilder, String socketBindingName) {
      this.serverConfigurationBuilder = serverConfigurationBuilder;
      this.attributes = EndpointConfiguration.attributeDefinitionSet();
      attributes.attribute(EndpointConfiguration.SOCKET_BINDING).set(socketBindingName);
      serverConfigurationBuilder.applySocketBinding(socketBindingName, singlePortBuilder, singlePortBuilder);
   }

   public EndpointConfigurationBuilder securityRealm(String name) {
      attributes.attribute(EndpointConfiguration.SECURITY_REALM).set(name);
      singlePortBuilder.securityRealm(serverConfigurationBuilder.getSecurityRealm(name));
      return this;
   }

   public EndpointConfigurationBuilder implicitConnectorSecurity(boolean implicitConnectorSecurity) {
      this.implicitConnectorSecurity = implicitConnectorSecurity;
      return this;
   }

   public EndpointConfigurationBuilder admin(boolean admin) {
      attributes.attribute(EndpointConfiguration.ADMIN).set(admin);
      return this;
   }

   public boolean admin() {
      return attributes.attribute(EndpointConfiguration.ADMIN).get();
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
      Map<String, List<ProtocolServerConfigurationBuilder<?, ?>>> buildersPerClass = connectorBuilders.stream()
            .collect(Collectors.groupingBy(s -> s.getClass().getSimpleName() + "/" + s.host() + ":" + s.port()));
      buildersPerClass.values().stream().filter(c -> c.size() > 1).findFirst().ifPresent(c -> {
         String names = c.stream().map(ProtocolServerConfigurationBuilder::name).collect(Collectors.joining(","));
         throw Server.log.multipleEndpointsSameTypeFound(names);
      });
   }

   @Override
   public EndpointConfiguration create() {
      boolean implicitSecurity = implicitConnectorSecurity && singlePortBuilder.securityRealm() != null;
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
      return new EndpointConfiguration(attributes.protect(), connectors, singlePortBuilder.create());
   }

   @Override
   public EndpointConfigurationBuilder read(EndpointConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   private void enableImplicitAuthentication(ServerSecurityRealm securityRealm, HotRodServerConfigurationBuilder builder) {
      // Set the security realm only if it has not been set already
      AuthenticationConfigurationBuilder authentication = builder.authentication();
      if (!authentication.hasSecurityRealm()) {
         authentication.securityRealm(securityRealm.getName());
         Server.log.debugf("Using endpoint realm \"%s\" for Hot Rod", securityRealm.getName());
      } else {
         securityRealm = serverConfigurationBuilder.getSecurityRealm(authentication.securityRealm());
      }
      // Only add implicit mechanisms if the user has not set any explicitly
      if (!authentication.hasMechanisms()) {
         String serverPrincipal = null;
         for (KerberosSecurityFactoryConfiguration identity : securityRealm.getServerIdentities().kerberosConfigurations()) {
            if (identity.getPrincipal().startsWith("hotrod/")) {
               authentication
                     .enable()
                     .addMechanisms(SaslMechanismInformation.Names.GS2_KRB5, SaslMechanismInformation.Names.GSSAPI);
               serverPrincipal = identity.getPrincipal();
               break;
            }
            Server.log.debugf("Enabled Kerberos mechanisms for Hot Rod using principal '%s'", identity.getPrincipal());
         }
         if (securityRealm.hasFeature(ServerSecurityRealm.Feature.TOKEN)) {
            authentication
                  .enable()
                  .addMechanisms(SaslMechanismInformation.Names.OAUTHBEARER);
            Server.log.debug("Enabled OAUTHBEARER mechanism for Hot Rod");
         }
         if (securityRealm.hasFeature(ServerSecurityRealm.Feature.TRUST)) {
            authentication
                  .enable()
                  .addMechanisms(SaslMechanismInformation.Names.EXTERNAL);
            Server.log.debug("Enabled EXTERNAL mechanism for Hot Rod");
         }
         if (securityRealm.hasFeature(ServerSecurityRealm.Feature.PASSWORD)) {
            authentication
                  .enable()
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

            // Only enable PLAIN if encryption is on
            if (singlePortBuilder.ssl().isEnabled()) {
               authentication
                     .enable()
                     .addMechanisms(SaslMechanismInformation.Names.PLAIN);
               Server.log.debug("Enabled PLAIN mechanism for Hot Rod");
            }
         }
         authentication.serverAuthenticationProvider(securityRealm.getSASLAuthenticationProvider(serverPrincipal));
      }
   }

   private void enableImplicitAuthentication(ServerSecurityRealm securityRealm, RestServerConfigurationBuilder builder) {
      // Set the security realm only if it has not been set already
      org.infinispan.rest.configuration.AuthenticationConfigurationBuilder authentication = builder.authentication();
      if (!authentication.hasSecurityRealm()) {
         authentication.securityRealm(securityRealm.getName());
      } else {
         securityRealm = serverConfigurationBuilder.getSecurityRealm(authentication.securityRealm());
      }
      // Only add implicit mechanisms if the user has not set any explicitly
      if (!authentication.hasMechanisms()) {
         String serverPrincipal = null;
         for (KerberosSecurityFactoryConfiguration identity : securityRealm.getServerIdentities().kerberosConfigurations()) {
            if (identity.getPrincipal().startsWith("HTTP/")) {
               authentication
                     .enable()
                     .addMechanisms("SPNEGO");
               serverPrincipal = identity.getPrincipal();
            }
            Server.log.debugf("Enabled SPNEGO authentication for HTTP using principal '%s'", identity.getPrincipal());
         }
         if (securityRealm.hasFeature(ServerSecurityRealm.Feature.TOKEN)) {
            authentication
                  .enable()
                  .addMechanisms("BEARER_TOKEN");
            Server.log.debug("Enabled BEARER_TOKEN for HTTP");
         }
         if (securityRealm.hasFeature(ServerSecurityRealm.Feature.TRUST)) {
            authentication
                  .enable()
                  .addMechanisms("CLIENT_CERT");
            Server.log.debug("Enabled CLIENT_CERT for HTTP");
         }
         if (securityRealm.hasFeature(ServerSecurityRealm.Feature.PASSWORD)) {
            authentication
                  .enable()
                  .addMechanisms("DIGEST");
            Server.log.debug("Enabled DIGEST for HTTP");

            // Only enable PLAIN if encryption is on
            if (singlePortBuilder.ssl().isEnabled()) {
               authentication
                     .enable()
                     .addMechanisms("BASIC");
               Server.log.debug("Enabled BASIC for HTTP");
            }
         }
         authentication.authenticator(securityRealm.getHTTPAuthenticationProvider(serverPrincipal));
      }
   }
}
