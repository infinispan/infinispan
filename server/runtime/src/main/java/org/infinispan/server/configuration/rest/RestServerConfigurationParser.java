package org.infinispan.server.configuration.rest;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import java.nio.file.Paths;

import javax.net.ssl.SSLContext;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.rest.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.rest.configuration.CorsConfigurationBuilder;
import org.infinispan.rest.configuration.CorsRuleConfigurationBuilder;
import org.infinispan.rest.configuration.ExtendedHeaders;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.ServerConfigurationParser;
import org.infinispan.server.configuration.endpoint.EndpointConfigurationBuilder;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.core.configuration.SniConfigurationBuilder;
import org.infinispan.server.security.ServerSecurityRealm;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * Server endpoint configuration parser
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
@MetaInfServices
@Namespaces({
      @Namespace(root = "rest-connector"),
      @Namespace(uri = "urn:infinispan:server:*", root = "rest-connector"),
})
public class RestServerConfigurationParser implements ConfigurationParser {
   private static org.infinispan.util.logging.Log coreLog = LogFactory.getLog(ServerConfigurationParser.class);

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      if (!holder.inScope(ServerConfigurationParser.ENDPOINTS_SCOPE)) {
         throw coreLog.invalidScope(ServerConfigurationParser.ENDPOINTS_SCOPE, holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case REST_CONNECTOR: {
            ServerConfigurationBuilder serverBuilder = builder.module(ServerConfigurationBuilder.class);
            if (serverBuilder != null) {
               parseRest(reader, serverBuilder);
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

   private void parseRest(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder)
         throws XMLStreamException {
      boolean dedicatedSocketBinding = false;
      boolean userDefinedName = false;
      EndpointConfigurationBuilder endpoint = serverBuilder.endpoints().current();
      RestServerConfigurationBuilder builder = endpoint.addConnector(RestServerConfigurationBuilder.class);
      if (endpoint.admin()) {
         String serverHome = reader.getProperties().getProperty(Server.INFINISPAN_SERVER_HOME_PATH);
         builder.staticResources(Paths.get(serverHome, Server.DEFAULT_SERVER_STATIC_DIR));
      }
      builder.authentication().metricsAuth(endpoint.metricsAuth());
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CONTEXT_PATH: {
               builder.contextPath(value);
               break;
            }
            case EXTENDED_HEADERS: {
               builder.extendedHeaders(ExtendedHeaders.valueOf(value));
               break;
            }
            case NAME: {
               builder.name(value);
               userDefinedName = true;
               break;
            }
            case MAX_CONTENT_LENGTH: {
               builder.maxContentLength(Integer.parseInt(value));
               break;
            }
            case COMPRESSION_LEVEL: {
               builder.compressionLevel(Integer.parseInt(value));
               break;
            }
            case SOCKET_BINDING: {
               builder.socketBinding(value);
               serverBuilder.applySocketBinding(value, builder, endpoint.singlePort());
               builder.startTransport(true);
               dedicatedSocketBinding = true;
               break;
            }
            default: {
               ServerConfigurationParser.parseCommonConnectorAttributes(reader, i, serverBuilder, builder);
            }
         }
      }
      if (!userDefinedName) {
         if (dedicatedSocketBinding) {
            builder.name("rest-" + builder.socketBinding());
         } else {
            builder.name("rest-" + endpoint.singlePort().socketBinding());
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case AUTHENTICATION: {
               parseAuthentication(reader, serverBuilder, builder.authentication().enable());
               break;
            }
            case ENCRYPTION: {
               if (!dedicatedSocketBinding) {
                  throw Server.log.cannotConfigureProtocolEncryptionUnderSinglePort();
               }
               parseEncryption(reader, serverBuilder, builder.encryption());
               break;
            }
            case CORS_RULES: {
               parseCorsRules(reader, builder);
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseCorsRules(XMLExtendedStreamReader reader, RestServerConfigurationBuilder builder)
         throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      CorsConfigurationBuilder cors = builder.cors();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CORS_RULE: {
               parseCorsRule(reader, cors.addNewRule());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseCorsRule(XMLExtendedStreamReader reader, CorsRuleConfigurationBuilder corsRule) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME: {
               corsRule.name(value);
               break;
            }
            case ALLOW_CREDENTIALS: {
               corsRule.allowCredentials(Boolean.parseBoolean(value));
               break;
            }
            case MAX_AGE_SECONDS: {
               corsRule.maxAge(Long.parseLong(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         String[] values = reader.getElementText().split(",");
         switch (element) {

            case ALLOWED_HEADERS: {
               corsRule.allowHeaders(values);
               break;
            }
            case ALLOWED_ORIGINS: {
               corsRule.allowOrigins(values);
               break;
            }
            case ALLOWED_METHODS: {
               corsRule.allowMethods(values);
               break;
            }
            case EXPOSE_HEADERS: {
               corsRule.exposeHeaders(values);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseAuthentication(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, AuthenticationConfigurationBuilder builder) throws XMLStreamException {
      ServerSecurityRealm securityRealm = serverBuilder.endpoints().current().singlePort().securityRealm();
      String serverPrincipal = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SECURITY_REALM: {
               builder.securityRealm(value);
               securityRealm = serverBuilder.getSecurityRealm(value);
               break;
            }
            case MECHANISMS: {
               builder.addMechanisms(reader.getListAttributeValue(i));
               break;
            }
            case SERVER_PRINCIPAL: {
               serverPrincipal = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ParseUtils.requireNoContent(reader);
      if (securityRealm == null) {
         throw Server.log.authenticationWithoutSecurityRealm();
      }
      builder.authenticator(securityRealm.getHTTPAuthenticationProvider(serverPrincipal));
   }

   private void parseEncryption(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, EncryptionConfigurationBuilder encryption) throws XMLStreamException {
      String securityRealm = ParseUtils.requireAttributes(reader, Attribute.SECURITY_REALM)[0];
      SSLContext sslContext = serverBuilder.getSSLContext(securityRealm);
      encryption.realm(securityRealm).sslContext(sslContext);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         String value = reader.getAttributeValue(i);
         switch (attribute) {
            case REQUIRE_SSL_CLIENT_AUTH: {
               encryption.requireClientAuth(Boolean.parseBoolean(value));
               break;
            }
            case SECURITY_REALM: {
               // Already seen
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      //Since nextTag() moves the pointer, we need to make sure we won't move too far
      boolean skipTagCheckAtTheEnd = reader.hasNext();

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SNI: {
               parseSni(reader, serverBuilder, encryption.addSni());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

      if (!skipTagCheckAtTheEnd)
         ParseUtils.requireNoContent(reader);
   }

   private void parseSni(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, SniConfigurationBuilder sni) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case HOST_NAME: {
               sni.host(value);
               break;
            }
            case SECURITY_REALM: {
               sni.realm(value);
               sni.sslContext(serverBuilder.getSSLContext(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

}
