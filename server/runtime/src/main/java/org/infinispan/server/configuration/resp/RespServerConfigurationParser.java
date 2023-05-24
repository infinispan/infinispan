package org.infinispan.server.configuration.resp;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.ServerConfigurationParser;
import org.infinispan.server.configuration.endpoint.EndpointConfigurationBuilder;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.resp.configuration.RespAuthenticationConfigurationBuilder;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.security.ElytronUsernamePasswordAuthenticator;
import org.infinispan.server.security.ElytronRESPAuthenticator;
import org.infinispan.server.security.RespClientCertAuthenticator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * Server endpoint configuration parser for resp protocol
 *
 * @author William Burns
 * @since 14.0
 */
@MetaInfServices
@Namespaces({
      @Namespace(root = "resp-connector"),
      @Namespace(uri = "urn:infinispan:server:*", root = "resp-connector"),
})
public class RespServerConfigurationParser implements ConfigurationParser {
   private static final Log coreLog = LogFactory.getLog(ServerConfigurationParser.class);

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      if (!holder.inScope(ServerConfigurationParser.ENDPOINTS_SCOPE)) {
         throw coreLog.invalidScope(ServerConfigurationParser.ENDPOINTS_SCOPE, holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case RESP_CONNECTOR: {
            ServerConfigurationBuilder serverBuilder = builder.module(ServerConfigurationBuilder.class);
            if (serverBuilder != null) {
               parseResp(reader, serverBuilder);
            } else {
               throw ParseUtils.unexpectedElement(reader);
            }
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   private void parseResp(ConfigurationReader reader, ServerConfigurationBuilder serverBuilder) {
      boolean dedicatedSocketBinding = false;
      String securityRealm = null;
      EndpointConfigurationBuilder endpoint = serverBuilder.endpoints().current();
      RespServerConfigurationBuilder builder = endpoint.addConnector(RespServerConfigurationBuilder.class);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case CACHE: {
               builder.defaultCacheName(value);
               break;
            }
            case NAME: {
               builder.name(value);
               break;
            }
            case SOCKET_BINDING: {
               builder.socketBinding(value);
               builder.startTransport(true);
               dedicatedSocketBinding = true;
               break;
            }
            case SECURITY_REALM: {
               securityRealm = value;
            }
            default: {
               ServerConfigurationParser.parseCommonConnectorAttributes(reader, i, serverBuilder, builder);
            }
         }
      }
      if (!dedicatedSocketBinding) {
         builder.socketBinding(endpoint.singlePort().socketBinding()).startTransport(false);
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case AUTHENTICATION: {
               parseAuthentication(reader, serverBuilder, builder.authentication().enable(), securityRealm);
               break;
            }
            case ENCRYPTION: {
               if (!dedicatedSocketBinding) {
                  throw Server.log.cannotConfigureProtocolEncryptionUnderSinglePort();
               }
               parseEncryption(reader, serverBuilder, builder.encryption(), securityRealm);
               break;
            }
            default: {
               ServerConfigurationParser.parseCommonConnectorElements(reader, builder);
            }
         }
      }
   }

   private void parseAuthentication(ConfigurationReader reader, ServerConfigurationBuilder serverBuilder, RespAuthenticationConfigurationBuilder builder, String securityRealmName) {
      if (securityRealmName == null) {
         securityRealmName = serverBuilder.endpoints().current().securityRealm();
      }
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case SECURITY_REALM: {
               builder.securityRealm(value);
               securityRealmName = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ParseUtils.requireNoContent(reader);
      if (securityRealmName == null) {
         throw Server.log.authenticationWithoutSecurityRealm();
      }
      ElytronRESPAuthenticator authenticator = new ElytronRESPAuthenticator();
      authenticator.withUsernamePasswordAuth(new ElytronUsernamePasswordAuthenticator(securityRealmName));
      authenticator.withClientCertAuth(new RespClientCertAuthenticator(securityRealmName));
      builder.authenticator(authenticator);
   }

   private void parseEncryption(ConfigurationReader reader, ServerConfigurationBuilder serverBuilder, EncryptionConfigurationBuilder encryption, String securityRealmName) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         org.infinispan.server.configuration.rest.Attribute attribute = org.infinispan.server.configuration.rest.Attribute.forName(reader.getAttributeName(i));
         String value = reader.getAttributeValue(i);
         switch (attribute) {
            case REQUIRE_SSL_CLIENT_AUTH: {
               encryption.requireClientAuth(Boolean.parseBoolean(value));
               break;
            }
            case SECURITY_REALM: {
               securityRealmName = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      if (securityRealmName == null) {
         throw Server.log.encryptionWithoutSecurityRealm();
      } else {
         encryption.realm(securityRealmName).sslContext(serverBuilder.serverSSLContextSupplier(securityRealmName));
      }

      ParseUtils.requireNoContent(reader);
   }

}
