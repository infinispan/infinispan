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
import org.infinispan.server.configuration.SocketBindingsConfiguration;
import org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration;
import org.infinispan.server.configuration.security.RealmConfiguration;
import org.infinispan.server.configuration.security.SecurityConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.security.ElytronHTTPAuthenticator;
import org.infinispan.server.security.ElytronRESPAuthenticator;
import org.infinispan.server.security.ElytronSASLAuthenticationProvider;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

/**
 * @since 12.0
 */
public class EndpointConfigurationBuilder implements Builder<EndpointConfiguration> {
   private final AttributeSet attributes;

   private final List<ProtocolServerConfigurationBuilder<?, ?>> connectorBuilders = new ArrayList<>(2);
   private final SinglePortServerConfigurationBuilder singlePortBuilder = new SinglePortServerConfigurationBuilder();
   private boolean implicitConnectorSecurity;

   public EndpointConfigurationBuilder(ServerConfigurationBuilder serverConfigurationBuilder, String socketBindingName) {
      this.attributes = EndpointConfiguration.attributeDefinitionSet();
      attributes.attribute(EndpointConfiguration.SOCKET_BINDING).set(socketBindingName);
      singlePortBuilder.socketBinding(socketBindingName);
   }

   public EndpointConfigurationBuilder securityRealm(String name) {
      attributes.attribute(EndpointConfiguration.SECURITY_REALM).set(name);
      return this;
   }

   public String securityRealm() {
      return attributes.attribute(EndpointConfiguration.SECURITY_REALM).get();
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

   public EndpointConfigurationBuilder metricsAuth(boolean auth) {
      attributes.attribute(EndpointConfiguration.METRICS_AUTH).set(auth);
      return this;
   }

   public boolean metricsAuth() {
      return attributes.attribute(EndpointConfiguration.METRICS_AUTH).get();
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
      throw new UnsupportedOperationException();
   }

   public EndpointConfiguration create(SocketBindingsConfiguration bindingsConfiguration, SecurityConfiguration securityConfiguration) {
      boolean implicitSecurity = implicitConnectorSecurity && securityRealm() != null;
      bindingsConfiguration.applySocketBinding(attributes.attribute(EndpointConfiguration.SOCKET_BINDING).get(), singlePortBuilder, singlePortBuilder);
      List<ProtocolServerConfiguration> connectors = new ArrayList<>(connectorBuilders.size());
      for (ProtocolServerConfigurationBuilder<?, ?> builder : connectorBuilders) {
         bindingsConfiguration.applySocketBinding(builder.socketBinding(), builder, singlePortBuilder);
         if (implicitSecurity) {
            if (builder instanceof HotRodServerConfigurationBuilder) {
               enableImplicitAuthentication(securityConfiguration, securityRealm(), (HotRodServerConfigurationBuilder) builder);
            } else if (builder instanceof RestServerConfigurationBuilder) {
               enableImplicitAuthentication(securityConfiguration, securityRealm(), (RestServerConfigurationBuilder) builder);
            } else if (builder instanceof RespServerConfigurationBuilder) {
               enableImplicitAuthentication(securityConfiguration, securityRealm(), (RespServerConfigurationBuilder) builder);
            }
         }
         connectors.add(builder.create());
      }
      if (implicitSecurity) {
         RealmConfiguration realm = securityConfiguration.realms().getRealm(securityRealm());
         if (realm.hasFeature(ServerSecurityRealm.Feature.ENCRYPT)) {
            singlePortBuilder.ssl().enable().sslContext(realm.serverSSLContext());
         }
      }
      return new EndpointConfiguration(attributes.protect(), connectors, singlePortBuilder.create());
   }

   @Override
   public EndpointConfigurationBuilder read(EndpointConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   public static void enableImplicitAuthentication(SecurityConfiguration security, String securityRealmName, HotRodServerConfigurationBuilder builder) {
      // Set the security realm only if it has not been set already
      AuthenticationConfigurationBuilder authentication = builder.authentication();
      if (!authentication.hasSecurityRealm()) {
         authentication.securityRealm(securityRealmName);
         Server.log.debugf("Using endpoint realm \"%s\" for Hot Rod", securityRealmName);
      }
      ServerSecurityRealm securityRealm = security.realms().getRealm(authentication.securityRealm()).serverSecurityRealm();
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
            if (securityRealm.hasFeature(ServerSecurityRealm.Feature.ENCRYPT)) {
               authentication
                     .enable()
                     .addMechanisms(SaslMechanismInformation.Names.PLAIN);
               Server.log.debug("Enabled PLAIN mechanism for Hot Rod");
            }
         }
         authentication.serverAuthenticationProvider(new ElytronSASLAuthenticationProvider(authentication.securityRealm(), serverPrincipal, authentication.sasl().mechanisms()));
      }
   }

   public static void enableImplicitAuthentication(SecurityConfiguration security, String securityRealmName, RestServerConfigurationBuilder builder) {
      // Set the security realm only if it has not been set already
      org.infinispan.rest.configuration.AuthenticationConfigurationBuilder authentication = builder.authentication();
      if (!authentication.hasSecurityRealm()) {
         authentication.securityRealm(securityRealmName);
      }
      ServerSecurityRealm securityRealm = security.realms().getRealm(authentication.securityRealm()).serverSecurityRealm();

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
            if (securityRealm.hasFeature(ServerSecurityRealm.Feature.ENCRYPT)) {
               authentication
                     .enable()
                     .addMechanisms("BASIC");
               Server.log.debug("Enabled BASIC for HTTP");
            }
         }
         authentication.authenticator(new ElytronHTTPAuthenticator(authentication.securityRealm(), serverPrincipal, authentication.mechanisms()));
      }
   }

   private void enableImplicitAuthentication(SecurityConfiguration security, String securityRealmName, RespServerConfigurationBuilder builder) {
      // Set the security realm only if it has not been set already
      org.infinispan.server.resp.configuration.AuthenticationConfigurationBuilder authentication = builder.authentication();
      if (!authentication.hasSecurityRealm()) {
         authentication.securityRealm(securityRealmName);
      }
      ServerSecurityRealm securityRealm = security.realms().getRealm(authentication.securityRealm()).serverSecurityRealm();
      if (securityRealm.hasFeature(ServerSecurityRealm.Feature.PASSWORD)) {
         authentication.authenticator(new ElytronRESPAuthenticator(authentication.securityRealm()));
      } else {
         throw Server.log.respEndpointRequiresRealmWithPassword();
      }
   }
}
