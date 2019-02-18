package org.infinispan.server.configuration;

import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.security.PropertiesSecurityRealm;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * Server endpoint configuration parser
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
@MetaInfServices
@Namespaces({
      @Namespace(root = "server"),
      @Namespace(uri = "urn:infinispan:server:*", root = "server"),
})
public class ServerConfigurationParser implements ConfigurationParser {
   private static org.infinispan.util.logging.Log coreLog = org.infinispan.util.logging.LogFactory.getLog(ServerConfigurationParser.class);

   public static String ENDPOINTS_SCOPE = "ENDPOINTS";

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   public static Element nextElement(XMLStreamReader reader) throws XMLStreamException {
      if (reader.nextTag() == END_ELEMENT) {
         return null;
      }
      return Element.forName(reader.getLocalName());
   }


   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      if (!holder.inScope(ParserScope.GLOBAL)) {
         throw coreLog.invalidScope(ParserScope.GLOBAL.name(), holder.getScope());
      }

      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case SERVER: {
            builder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
            ServerConfigurationBuilder serverConfigurationBuilder = builder.addModule(ServerConfigurationBuilder.class);
            parseServerElements(reader, holder, serverConfigurationBuilder);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseServerElements(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, ServerConfigurationBuilder builder)
         throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case INTERFACES: {
               parseInterfaces(reader, builder);
               break;
            }
            case SOCKET_BINDINGS: {
               parseSocketBindings(reader, builder);
               break;
            }
            case SECURITY: {
               parseSecurity(reader, builder);
               break;
            }
            case ENDPOINTS: {
               parseEndpoints(reader, holder);
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSocketBindings(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.DEFAULT_INTERFACE, Attribute.PORT_OFFSET);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SOCKET_BINDING: {
               parseSocketBinding(reader, builder, attributes[0], Integer.parseInt(attributes[1]));
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSocketBinding(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder, String defaultInterfaceName, int portOffset) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.PORT);
      String name = attributes[0];
      int port = Integer.parseInt(attributes[1]);
      String interfaceName = defaultInterfaceName;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
            case PORT:
               // already parsed
               break;
            case INTERFACE:
               interfaceName = value;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      builder.addSocketBinding(name, interfaceName, port + portOffset);

   }

   private void parseInterfaces(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case INTERFACE:
               parseInterface(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseInterface(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME);
      String name = attributes[0];

      Element element = nextElement(reader);
      if (element == null) {
         throw ParseUtils.unexpectedEndElement(reader);
      }
      switch (element) {
         case INET_ADDRESS:
            ParseUtils.requireNoContent(reader);
            builder.addNetworkInterface(NetworkAddress.inetAddress(name, ParseUtils.requireSingleAttribute(reader, Attribute.VALUE)));
            break;
         case LINK_LOCAL:
            ParseUtils.requireNoAttributes(reader);
            ParseUtils.requireNoContent(reader);
            builder.addNetworkInterface(NetworkAddress.linkLocalAddress(name));
            break;
         case GLOBAL:
            ParseUtils.requireNoAttributes(reader);
            ParseUtils.requireNoContent(reader);
            builder.addNetworkInterface(NetworkAddress.globalAddress(name));
            break;
         case LOOPBACK:
            ParseUtils.requireNoAttributes(reader);
            ParseUtils.requireNoContent(reader);
            builder.addNetworkInterface(NetworkAddress.loopback(name));
            break;
         case NON_LOOPBACK:
            ParseUtils.requireNoAttributes(reader);
            ParseUtils.requireNoContent(reader);
            builder.addNetworkInterface(NetworkAddress.nonLoopback(name));
            break;
         case SITE_LOCAL:
            ParseUtils.requireNoAttributes(reader);
            ParseUtils.requireNoContent(reader);
            builder.addNetworkInterface(NetworkAddress.siteLocal(name));
            break;
         case MATCH_INTERFACE:
            ParseUtils.requireNoContent(reader);
            builder.addNetworkInterface(NetworkAddress.matchInterface(name, ParseUtils.requireSingleAttribute(reader, Attribute.VALUE)));
            break;
         case MATCH_ADDRESS:
            ParseUtils.requireNoContent(reader);
            builder.addNetworkInterface(NetworkAddress.matchAddress(name, ParseUtils.requireSingleAttribute(reader, Attribute.VALUE)));
            break;
         case MATCH_HOST:
            ParseUtils.requireNoContent(reader);
            builder.addNetworkInterface(NetworkAddress.matchHost(name, ParseUtils.requireSingleAttribute(reader, Attribute.VALUE)));
            break;
         default:
            throw ParseUtils.unexpectedElement(reader);
      }
      ParseUtils.requireNoContent(reader); // Consume the </interface> tag
   }

   private void parseSecurity(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SECURITY_REALMS:
               parseSecurityRealms(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSecurityRealms(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SECURITY_REALM:
               parseSecurityRealm(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSecurityRealm(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      // TODO: ISPN-9944 Complete parsing and implementation of the Security Realms
      String name = ParseUtils.requireSingleAttribute(reader, Attribute.NAME);
      Map<Class<? extends SecurityRealm>, SecurityRealm> subRealms = new LinkedHashMap<>();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case KERBEROS_REALM:
               SecurityRealm realm = parseKerberosRealm(reader, builder);
               break;
            case LOCAL_REALM:
               realm = parseLocalRealm(reader, builder);
               break;
            case PROPERTIES_REALM:
               realm = parsePropertiesRealm(reader, name);
               break;
            case SERVER_IDENTITIES:
               parseServerIdentitities(reader, builder);
               break;
            case TRUSTSTORE_REALM:
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private SecurityRealm parseKerberosRealm(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      String keyTab = ParseUtils.requireAttributes(reader, Attribute.KEYTAB)[0];
      ParseUtils.requireNoContent(reader);
      return null;
   }

   private SecurityRealm parseLocalRealm(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      ParseUtils.requireNoContent(reader);
      return null;
   }

   private SecurityRealm parsePropertiesRealm(XMLExtendedStreamReader reader, String name) throws XMLStreamException {
      File usersFile = null;
      File groupsFile = null;
      boolean plainText = true;

      Element element = nextElement(reader);
      if (element == Element.USER_PROPERTIES) {
         usersFile = new File(ParseUtils.requireAttributes(reader, Attribute.PATH)[0]);
         ParseUtils.requireNoContent(reader);
         element = nextElement(reader);
      }
      if (element == Element.GROUP_PROPERTIES) {
         groupsFile = new File(ParseUtils.requireAttributes(reader, Attribute.PATH)[0]);
         ParseUtils.requireNoContent(reader);
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader);
      }
      return new PropertiesSecurityRealm(usersFile, groupsFile, plainText, "Roles", name);
   }

   private void parseServerIdentitities(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SSL:
               parseSSL(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSSL(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ENGINE:
               parseSSLEngine(reader, builder);
               break;
            case KEYSTORE:
               parseKeyStore(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSSLEngine(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED_PROTOCOLS:
               break;
            case ENABLED_CIPHERSUITES:
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseKeyStore(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.PATH);
      String keyStoreFileName = attributes[0];
      String keyStorePassword = null;
      String keyStoreAlias = null;
      String keyStoreKeyPassword = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PATH:
               // Already seen
               break;
            case RELATIVE_TO:
               break;
            case KEYSTORE_PASSWORD:
               keyStorePassword = value;
               break;
            case ALIAS:
               keyStoreAlias = value;
               break;
            case KEY_PASSWORD:
               keyStoreKeyPassword = value;
               break;
            case GENERATE_SELF_SIGNED_CERTIFICATE_HOST:
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseEndpoints(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      holder.pushScope(ENDPOINTS_SCOPE);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         reader.handleAny(holder);
      }
      holder.popScope();
   }
}
