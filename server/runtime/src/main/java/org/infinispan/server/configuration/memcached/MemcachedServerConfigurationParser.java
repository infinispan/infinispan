package org.infinispan.server.configuration.memcached;

import static org.infinispan.server.configuration.ServerConfigurationParser.parseSasl;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.ServerConfigurationParser;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.memcached.configuration.MemcachedAuthenticationConfigurationBuilder;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.server.security.ElytronSASLAuthenticator;
import org.infinispan.server.security.ElytronUsernamePasswordAuthenticator;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.sasl.WildFlySasl;

/**
 * Server endpoint configuration parser for memcached
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@MetaInfServices
@Namespaces({
      @Namespace(root = "memcached-connector"),
      @Namespace(uri = "urn:infinispan:server:*", root = "memcached-connector"),
})
public class MemcachedServerConfigurationParser implements ConfigurationParser {
   private static final org.infinispan.util.logging.Log coreLog = org.infinispan.util.logging.LogFactory.getLog(ServerConfigurationParser.class);

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder)
         {
      if (!holder.inScope(ServerConfigurationParser.ENDPOINTS_SCOPE)) {
         throw coreLog.invalidScope(ServerConfigurationParser.ENDPOINTS_SCOPE, holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case MEMCACHED_CONNECTOR: {
            ServerConfigurationBuilder serverBuilder = builder.module(ServerConfigurationBuilder.class);
            if (serverBuilder != null) {
               parseMemcached(reader, serverBuilder, serverBuilder.endpoints().current().addConnector(MemcachedServerConfigurationBuilder.class));
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

   private void parseMemcached(ConfigurationReader reader, ServerConfigurationBuilder serverBuilder, MemcachedServerConfigurationBuilder builder) {
      boolean dedicatedSocketBinding = false;
      String securityRealm = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case CACHE: {
               builder.cache(value);
               break;
            }
            case CACHE_CONTAINER: {
               break;
            }
            case CLIENT_ENCODING: {
               builder.clientEncoding(MediaType.fromString(value));
               break;
            }
            case IDLE_TIMEOUT: {
               builder.idleTimeout(Integer.parseInt(value));
               break;
            }
            case IO_THREADS: {
               builder.ioThreads(Integer.parseInt(value));
               break;
            }
            case NAME: {
               builder.name(value);
               break;
            }
            case PROTOCOL: {
               builder.protocol(ParseUtils.parseEnum(reader, i, MemcachedProtocol.class, value));
               break;
            }
            case SECURITY_REALM: {
               builder.authentication().securityRealm(value);
               break;
            }
            case SOCKET_BINDING: {
               builder.socketBinding(value);
               builder.startTransport(true);
               dedicatedSocketBinding = true;
               break;
            }
            default: {
               ServerConfigurationParser.parseCommonConnectorAttributes(reader, i, serverBuilder, builder);
            }
         }
      }
      if (!dedicatedSocketBinding) {
         builder.socketBinding(serverBuilder.endpoints().current().singlePort().socketBinding()).startTransport(false);
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

   private void parseAuthentication(ConfigurationReader reader, ServerConfigurationBuilder serverBuilder, MemcachedAuthenticationConfigurationBuilder builder, String securityRealm) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         org.infinispan.server.hotrod.configuration.Attribute attribute = org.infinispan.server.hotrod.configuration.Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case SECURITY_REALM: {
               securityRealm = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      if (securityRealm == null) {
         securityRealm = serverBuilder.endpoints().current().securityRealm();
      }
      if (securityRealm == null) {
         throw Server.log.authenticationWithoutSecurityRealm();
      }
      // Automatically set the digest realm name. It can be overridden by the user
      builder.sasl().addMechProperty(WildFlySasl.REALM_LIST, securityRealm);
      String serverPrincipal = null;
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SASL: {
               serverPrincipal = parseSasl(reader, builder.sasl());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      builder.securityRealm(securityRealm);
      builder.sasl().authenticator(new ElytronSASLAuthenticator(securityRealm, serverPrincipal, builder.sasl().mechanisms()));
      builder.text().authenticator(new ElytronUsernamePasswordAuthenticator(securityRealm));
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
