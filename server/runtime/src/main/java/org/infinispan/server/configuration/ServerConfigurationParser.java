package org.infinispan.server.configuration;

import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;

import java.io.File;
import java.io.FileInputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.server.Server;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.security.KeyStoreUtils;
import org.infinispan.server.security.realm.KerberosSecurityRealm;
import org.infinispan.server.security.realm.PropertiesSecurityRealm;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.keystore.AliasFilter;
import org.wildfly.security.keystore.FilteringKeyStore;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.ssl.CipherSuiteSelector;
import org.wildfly.security.ssl.ProtocolSelector;
import org.wildfly.security.ssl.SSLContextBuilder;
import org.wildfly.security.util.ProviderUtil;

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
      String name = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
      SecurityDomain.Builder domainBuilder = SecurityDomain.builder();
      SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
      boolean hasTrustStore = false;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case KERBEROS_REALM:
               parseKerberosRealm(reader, domainBuilder);
               break;
            case LDAP_REALM:
               parseLdapRealm(reader, domainBuilder);
               break;
            case LOCAL_REALM:
               parseLocalRealm(reader, domainBuilder);
               break;
            case PROPERTIES_REALM:
               parsePropertiesRealm(reader, domainBuilder);
               break;
            case SERVER_IDENTITIES:
               parseServerIdentitities(reader, sslContextBuilder);
               break;
            case TRUSTSTORE_REALM:
               parseTrustStoreRealm(reader, sslContextBuilder);
               hasTrustStore = true;
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      SecurityDomain securityDomain = domainBuilder.build();
      builder.addSecurityDomain(name, securityDomain);
      if (hasTrustStore) {
         sslContextBuilder.setSecurityDomain(securityDomain);
      }
      try {
         builder.addSSLContext(name, sslContextBuilder.build().create());
      } catch (GeneralSecurityException e) {
         throw new CacheConfigurationException(e);
      }
   }

   private void parseKerberosRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      String keyTab = ParseUtils.requireAttributes(reader, Attribute.KEYTAB)[0];
      ParseUtils.requireNoContent(reader);
      domainBuilder.addRealm("kerberos", new KerberosSecurityRealm(new File(keyTab)));
   }

   private void parseLdapRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               // Already seen
               break;
            case DIRECT_VERIFICATION:
               break;
            case ALLOW_BLANK_PASSWORD:
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseLocalRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      ParseUtils.requireNoContent(reader);
   }

   private void parsePropertiesRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      File usersFile = null;
      File groupsFile = null;
      boolean plainText = false;
      String realmName = null;
      String groupsAttribute = "groups";

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case GROUPS_ATTRIBUTE:
               groupsAttribute = value;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      Element element = nextElement(reader);
      if (element == Element.USER_PROPERTIES) {
         String path = ParseUtils.requireAttributes(reader, Attribute.PATH)[0];
         String relativeTo = (String) reader.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH);
         for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
               case PATH:
                  // Already seen
                  break;
               case RELATIVE_TO:
                  relativeTo = ParseUtils.requireAttributeProperty(reader, i);
                  break;
               case DIGEST_REALM_NAME:
                  realmName = value;
                  break;
               case PLAIN_TEXT:
                  plainText = Boolean.parseBoolean(value);
                  break;
               default:
                  throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
         usersFile = new File(ParseUtils.resolvePath(path, relativeTo));
         ParseUtils.requireNoContent(reader);
         element = nextElement(reader);
      }
      if (element == Element.GROUP_PROPERTIES) {
         String path = ParseUtils.requireAttributes(reader, Attribute.PATH)[0];
         String relativeTo = (String) reader.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH);
         for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
               case PATH:
                  // Already seen
                  break;
               case RELATIVE_TO:
                  relativeTo = ParseUtils.requireAttributeProperty(reader, i);
                  break;
               default:
                  throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }

         groupsFile = new File(ParseUtils.resolvePath(path, relativeTo));
         ParseUtils.requireNoContent(reader);
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader);
      }
      domainBuilder.addRealm("properties",
            new PropertiesSecurityRealm(usersFile, groupsFile, plainText, groupsAttribute, realmName));
   }

   private void parseServerIdentitities(XMLExtendedStreamReader reader, SSLContextBuilder sslContextBuilder) throws
         XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SSL:
               parseSSL(reader, sslContextBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSSL(XMLExtendedStreamReader reader, SSLContextBuilder sslContextBuilder) throws
         XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ENGINE:
               parseSSLEngine(reader, sslContextBuilder);
               break;
            case KEYSTORE:
               parseKeyStore(reader, sslContextBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSSLEngine(XMLExtendedStreamReader reader, SSLContextBuilder sslContextBuilder) throws
         XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED_PROTOCOLS:
               ProtocolSelector protocolSelector = ProtocolSelector.empty();
               for(String protocol : reader.getListAttributeValue(i)) {
                  protocolSelector.add(protocol);
               }
               sslContextBuilder.setProtocolSelector(protocolSelector);
               break;
            case ENABLED_CIPHERSUITES:
               sslContextBuilder.setCipherSuiteSelector(CipherSuiteSelector.fromString(reader.getAttributeValue(i)));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseKeyStore(XMLExtendedStreamReader reader, SSLContextBuilder sslContextBuilder) throws
         XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.PATH);
      String path = attributes[0];
      String relativeTo = (String) reader.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH);
      String keyStoreProvider = null;
      char[] keyStorePassword = null;
      String keyAlias = null;
      char[] keyPassword = null;
      String generateSelfSignedHost = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PATH:
               // Already seen
               break;
            case PROVIDER:
               keyStoreProvider = value;
               break;
            case RELATIVE_TO:
               relativeTo = ParseUtils.requireAttributeProperty(reader, i);
               break;
            case KEYSTORE_PASSWORD:
               keyStorePassword = value.toCharArray();
               break;
            case ALIAS:
               keyAlias = value;
               break;
            case KEY_PASSWORD:
               keyPassword = value.toCharArray();
               break;
            case GENERATE_SELF_SIGNED_CERTIFICATE_HOST:
               generateSelfSignedHost = value;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      String keyStoreFileName = ParseUtils.resolvePath(path, relativeTo);
      try {
         if (!new File(keyStoreFileName).exists() && generateSelfSignedHost != null) {
            KeyStoreUtils.generateSelfSignedCertificate(keyStoreFileName, keyStoreProvider, keyStorePassword, keyPassword, keyAlias, generateSelfSignedHost);
         }
         KeyStore keyStore = KeyStoreUtil.loadKeyStore(ProviderUtil.INSTALLED_PROVIDERS, keyStoreProvider, new FileInputStream(keyStoreFileName), keyStoreFileName, keyStorePassword);
         if (keyAlias != null) {
            keyStore = FilteringKeyStore.filteringKeyStore(keyStore, AliasFilter.fromString(keyAlias));
         }
         KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
         kmf.init(keyStore, keyPassword);
         for (KeyManager keyManager : kmf.getKeyManagers()) {
            if (keyManager instanceof X509ExtendedKeyManager) {
               sslContextBuilder.setKeyManager((X509ExtendedKeyManager) keyManager);
               return;
            }
         }
         throw Server.log.noDefaultKeyManager();
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }

   private void parseTrustStoreRealm(XMLExtendedStreamReader reader, SSLContextBuilder sslContextBuilder) throws
         XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.PATH);
      String path = attributes[0];
      String relativeTo = (String) reader.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH);
      String keyStoreProvider = null;
      char[] keyStorePassword = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PATH:
               // Already seen
               break;
            case PROVIDER:
               keyStoreProvider = value;
               break;
            case KEYSTORE_PASSWORD:
               keyStorePassword = value.toCharArray();
               break;
            case RELATIVE_TO:
               relativeTo = ParseUtils.requireAttributeProperty(reader, i);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      String trustStoreFileName = ParseUtils.resolvePath(path, relativeTo);
      try {
         KeyStore keyStore = KeyStoreUtil.loadKeyStore(ProviderUtil.INSTALLED_PROVIDERS, keyStoreProvider, new FileInputStream(trustStoreFileName), trustStoreFileName, keyStorePassword);
         TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
         tmf.init(keyStore);
         for (TrustManager trustManager : tmf.getTrustManagers()) {
            if (trustManager instanceof X509TrustManager) {
               sslContextBuilder.setTrustManager((X509TrustManager) trustManager);
               return;
            }
         }
         throw Server.log.noDefaultKeyManager();
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }

   private void parseEndpoints(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws
         XMLStreamException {
      holder.pushScope(ENDPOINTS_SCOPE);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         reader.handleAny(holder);
      }
      holder.popScope();
   }
}
