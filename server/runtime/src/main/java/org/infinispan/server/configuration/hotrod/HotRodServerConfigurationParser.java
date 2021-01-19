package org.infinispan.server.configuration.hotrod;

import java.util.EnumSet;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.ServerConfigurationParser;
import org.infinispan.server.configuration.endpoint.EndpointConfigurationBuilder;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.core.configuration.SniConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.PolicyConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.SaslConfigurationBuilder;
import org.infinispan.server.security.ServerSecurityRealm;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.sasl.WildFlySasl;

/**
 * Server endpoint configuration parser
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
@MetaInfServices
@Namespaces({
      @Namespace(root = "hotrod-connector"),
      @Namespace(uri = "urn:infinispan:server:*", root = "hotrod-connector"),
})
public class HotRodServerConfigurationParser implements ConfigurationParser {
   private static org.infinispan.util.logging.Log coreLog = org.infinispan.util.logging.LogFactory.getLog(ServerConfigurationParser.class);

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      if (!holder.inScope(ServerConfigurationParser.ENDPOINTS_SCOPE)) {
         throw coreLog.invalidScope(ServerConfigurationParser.ENDPOINTS_SCOPE, holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case HOTROD_CONNECTOR: {
            ServerConfigurationBuilder serverBuilder = builder.module(ServerConfigurationBuilder.class);
            if (serverBuilder != null) {
               parseHotRodConnector(reader, holder, serverBuilder, serverBuilder.endpoints().current().addConnector(HotRodServerConfigurationBuilder.class));
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

   private void parseHotRodConnector(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, ServerConfigurationBuilder serverBuilder, HotRodServerConfigurationBuilder builder)
         throws XMLStreamException {
      boolean dedicatedSocketBinding = false;
      ServerSecurityRealm securityRealm = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case EXTERNAL_HOST: {
               builder.proxyHost(value);
               break;
            }
            case EXTERNAL_PORT: {
               builder.proxyPort(Integer.parseInt(value));
               break;
            }
            case NAME: {
               builder.name(value);
               break;
            }
            case SOCKET_BINDING: {
               builder.socketBinding(value);
               serverBuilder.applySocketBinding(value, builder, serverBuilder.endpoints().current().singlePort());
               builder.startTransport(true);
               dedicatedSocketBinding = true;
               break;
            }
            case SECURITY_REALM: {
                securityRealm = serverBuilder.getSecurityRealm(value);
            }
            default: {
               ServerConfigurationParser.parseCommonConnectorAttributes(reader, i, serverBuilder, builder);
            }
         }
      }
      if (!dedicatedSocketBinding) {
         builder.socketBinding(serverBuilder.endpoints().current().singlePort().socketBinding());
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case TOPOLOGY_STATE_TRANSFER: {
               parseTopologyStateTransfer(reader, builder);
               break;
            }
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
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      if (securityRealm != null) {
         EndpointConfigurationBuilder.enableImplicitAuthentication(serverBuilder, securityRealm, builder);
      }
   }

   private void parseEncryption(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, EncryptionConfigurationBuilder encryption, ServerSecurityRealm securityRealm) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case REQUIRE_SSL_CLIENT_AUTH: {
               encryption.requireClientAuth(Boolean.parseBoolean(value));
               break;
            }
            case SECURITY_REALM: {
               securityRealm = serverBuilder.getSecurityRealm(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      if (securityRealm == null) {
         throw Server.log.encryptionWithoutSecurityRealm();
      } else {
         String name = securityRealm.getName();
         encryption.realm(name).sslContext(serverBuilder.getSSLContext(name));
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
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

   private void parseAuthentication(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, AuthenticationConfigurationBuilder builder, ServerSecurityRealm securityRealm) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SECURITY_REALM: {
               securityRealm = serverBuilder.getSecurityRealm(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      if (securityRealm == null) {
         securityRealm = serverBuilder.endpoints().current().singlePort().securityRealm();
      }
      if (securityRealm == null) {
         throw Server.log.authenticationWithoutSecurityRealm();
      }
      // Automatically set the digest realm name. It can be overridden by the user
      builder.addMechProperty(WildFlySasl.REALM_LIST, securityRealm.getName());
      String serverPrincipal = null;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SASL: {
               serverPrincipal = parseSasl(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      builder.securityRealm(securityRealm.getName());
      builder.serverAuthenticationProvider(securityRealm.getSASLAuthenticationProvider(serverPrincipal, builder.sasl().mechanisms()));
   }

   private String parseSasl(XMLExtendedStreamReader reader, AuthenticationConfigurationBuilder builder) throws XMLStreamException {
      SaslConfigurationBuilder sasl = builder.sasl();
      String serverPrincipal = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SERVER_PRINCIPAL: {
               serverPrincipal = value;
               break;
            }
            case SERVER_NAME: {
               sasl.serverName(value);
               break;
            }
            case MECHANISMS: {
               for (String mech : reader.getListAttributeValue(i)) {
                  sasl.addAllowedMech(mech);
               }
               break;
            }
            case QOP: {
               for (String qop : reader.getListAttributeValue(i)) {
                  sasl.addQOP(qop);
               }
               break;
            }
            case STRENGTH: {
               for (String s : reader.getListAttributeValue(i)) {
                  sasl.addStrength(s);
               }
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      final EnumSet<Element> visited = EnumSet.noneOf(Element.class);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case POLICY: {
               if (visited.contains(element)) {
                  throw ParseUtils.unexpectedElement(reader);
               } else {
                  visited.add(element);
               }
               parsePolicy(reader, builder);
               break;
            }
            case PROPERTY: {
               sasl.addProperty(ParseUtils.requireSingleAttribute(reader, Attribute.NAME), reader.getElementText());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      return serverPrincipal;
   }

   void parsePolicy(XMLExtendedStreamReader reader, AuthenticationConfigurationBuilder builder) throws XMLStreamException {
      if (reader.getAttributeCount() > 0) {
         throw ParseUtils.unexpectedAttribute(reader, 0);
      }
      PolicyConfigurationBuilder policy = builder.sasl().policy();
      // Handle nested elements.
      final EnumSet<Element> visited = EnumSet.noneOf(Element.class);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         if (visited.contains(element)) {
            throw ParseUtils.unexpectedElement(reader);
         }
         visited.add(element);
         String value = ParseUtils.readStringAttributeElement(reader, Attribute.VALUE.toString());
         switch (element) {
            case FORWARD_SECRECY: {
               policy.forwardSecrecy().value(Boolean.parseBoolean(value));
               break;
            }
            case NO_ACTIVE: {
               policy.noActive().value(Boolean.parseBoolean(value));
               break;
            }
            case NO_ANONYMOUS: {
               policy.noAnonymous().value(Boolean.parseBoolean(value));
               break;
            }
            case NO_DICTIONARY: {
               policy.noDictionary().value(Boolean.parseBoolean(value));
               break;
            }
            case NO_PLAIN_TEXT: {
               policy.noPlainText().value(Boolean.parseBoolean(value));
               break;
            }
            case PASS_CREDENTIALS: {
               policy.passCredentials().value(Boolean.parseBoolean(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseTopologyStateTransfer(XMLExtendedStreamReader reader, HotRodServerConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case LOCK_TIMEOUT: {
               builder.topologyLockTimeout(Long.parseLong(value));
               break;
            }
            case AWAIT_INITIAL_RETRIEVAL: {
               builder.topologyAwaitInitialTransfer(Boolean.parseBoolean(value));
               break;
            }
            case REPLICATION_TIMEOUT: {
               builder.topologyReplTimeout(Long.parseLong(value));
               break;
            }
            case LAZY_RETRIEVAL: {
               if (reader.getSchema().since(11, 0))
                  Server.log.warnHotRodLazyRetrievalDeprecated();
               builder.topologyStateTransfer(!Boolean.parseBoolean(value));
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
