package org.infinispan.server.configuration.hotrod;

import java.util.EnumSet;

import javax.security.sasl.Sasl;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.ServerConfigurationParser;
import org.infinispan.server.core.configuration.SslConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.security.ElytronAuthenticationProvider;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.auth.server.SecurityDomain;

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
               parseHotRodConnector(reader, holder, serverBuilder, serverBuilder.addEndpoint(HotRodServerConfigurationBuilder.class));
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
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CACHE_CONTAINER: {
               // TODO: add support for multiple containers
               break;
            }
            case EXTERNAL_HOST: {
               builder.proxyHost(value);
               break;
            }
            case EXTERNAL_PORT: {
               builder.proxyPort(Integer.parseInt(value));
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
            case RECEIVE_BUFFER_SIZE: {
               builder.recvBufSize(Integer.parseInt(value));
               break;
            }
            case SECURITY_REALM: {
               break;
            }
            case SEND_BUFFER_SIZE: {
               builder.sendBufSize(Integer.parseInt(value));
               break;
            }
            case SOCKET_BINDING: {
               serverBuilder.applySocketBinding(value, builder);
               break;
            }
            case TCP_KEEPALIVE: {
               builder.tcpKeepAlive(Boolean.parseBoolean(value));
               break;
            }
            case TCP_NODELAY: {
               builder.tcpNoDelay(Boolean.parseBoolean(value));
               break;
            }
            case WORKER_THREADS: {
               builder.workerThreads(Integer.parseInt(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case TOPOLOGY_STATE_TRANSFER: {
               parseTopologyStateTransfer(reader, builder);
               break;
            }
            case AUTHENTICATION: {
               parseAuthentication(reader, serverBuilder, builder.authentication().enable());
               break;
            }
            case ENCRYPTION: {
               parseEncryption(reader, serverBuilder, builder.ssl().enable());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseEncryption(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, SslConfigurationBuilder builder) throws XMLStreamException {
      String securityRealm = ParseUtils.requireAttributes(reader, Attribute.SECURITY_REALM)[0];
      builder.sslContext(serverBuilder.getSSLContext(securityRealm));
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case REQUIRE_SSL_CLIENT_AUTH: {
               builder.requireClientAuth(Boolean.parseBoolean(value));
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
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SNI: {
               parseSni(reader, serverBuilder, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseSni(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, SslConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case HOST_NAME: {
               builder.sniHostName(value);
               break;
            }
            case SECURITY_REALM: {
               builder.sslContext(serverBuilder.getSSLContext(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseAuthentication(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, AuthenticationConfigurationBuilder builder) throws XMLStreamException {
      String securityRealm = ParseUtils.requireAttributes(reader, Attribute.SECURITY_REALM)[0];
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SECURITY_REALM: {
               // Already seen
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SASL: {
               parseSasl(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      SecurityDomain securityDomain = serverBuilder.getSecurityDomain(securityRealm);
      builder.serverAuthenticationProvider(new ElytronAuthenticationProvider(securityDomain));
   }

   private void parseSasl(XMLExtendedStreamReader reader, AuthenticationConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SERVER_CONTEXT_NAME: {
               //TODO: Kerberos auth
               break;
            }
            case SERVER_NAME: {
               builder.serverName(value);
               break;
            }
            case MECHANISMS: {
               for (String mech : reader.getListAttributeValue(i)) {
                  builder.addAllowedMech(mech);
               }
               break;
            }
            case QOP: {
               builder.addMechProperty(Sasl.QOP, value);
               break;
            }
            case STRENGTH: {
               builder.addMechProperty(Sasl.STRENGTH, value);
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
               builder.addMechProperty(ParseUtils.requireSingleAttribute(reader, Attribute.NAME.name()), reader.getElementText());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

   }

   void parsePolicy(XMLExtendedStreamReader reader, AuthenticationConfigurationBuilder builder) throws XMLStreamException {
      if (reader.getAttributeCount() > 0) {
         throw ParseUtils.unexpectedAttribute(reader, 0);
      }
      // Handle nested elements.
      final EnumSet<Element> visited = EnumSet.noneOf(Element.class);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         if (visited.contains(element)) {
            throw ParseUtils.unexpectedElement(reader);
         }
         visited.add(element);
         String value = ParseUtils.readStringAttributeElement(reader, Attribute.VALUE.name());
         switch (element) {
            case FORWARD_SECRECY: {
               builder.addMechProperty(Sasl.POLICY_FORWARD_SECRECY, value);
               break;
            }
            case NO_ACTIVE: {
               builder.addMechProperty(Sasl.POLICY_NOACTIVE, value);
               break;
            }
            case NO_ANONYMOUS: {
               builder.addMechProperty(Sasl.POLICY_NOANONYMOUS, value);
               break;
            }
            case NO_DICTIONARY: {
               builder.addMechProperty(Sasl.POLICY_NODICTIONARY, value);
               break;
            }
            case NO_PLAIN_TEXT: {
               builder.addMechProperty(Sasl.POLICY_NOPLAINTEXT, value);
               break;
            }
            case PASS_CREDENTIALS: {
               builder.addMechProperty(Sasl.POLICY_PASS_CREDENTIALS, value);
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
               builder.topologyStateTransfer(Boolean.parseBoolean(value));
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
