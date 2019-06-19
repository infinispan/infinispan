package org.infinispan.server.configuration;

import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;

import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.security.HostnameVerificationPolicy;
import org.infinispan.server.security.KeyStoreUtils;
import org.infinispan.server.security.realm.KerberosSecurityRealm;
import org.infinispan.server.security.realm.PropertiesSecurityRealm;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.auth.permission.LoginPermission;
import org.wildfly.security.auth.realm.FileSystemSecurityRealm;
import org.wildfly.security.auth.realm.ldap.AttributeMapping;
import org.wildfly.security.auth.realm.ldap.DirContextFactory;
import org.wildfly.security.auth.realm.ldap.LdapSecurityRealmBuilder;
import org.wildfly.security.auth.realm.ldap.SimpleDirContextFactoryBuilder;
import org.wildfly.security.auth.realm.token.TokenSecurityRealm;
import org.wildfly.security.auth.realm.token.validator.JwtValidator;
import org.wildfly.security.auth.realm.token.validator.OAuth2IntrospectValidator;
import org.wildfly.security.auth.server.NameRewriter;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.keystore.AliasFilter;
import org.wildfly.security.keystore.FilteringKeyStore;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.permission.PermissionVerifier;
import org.wildfly.security.provider.util.ProviderUtil;
import org.wildfly.security.ssl.CipherSuiteSelector;
import org.wildfly.security.ssl.ProtocolSelector;
import org.wildfly.security.ssl.SSLContextBuilder;

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
               parseEndpoints(reader, holder, builder);
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
            case FILESYSTEM_REALM:
               parseFileSystemRealm(reader, domainBuilder);
               break;
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
            case TOKEN_REALM:
               parseTokenRealm(reader, domainBuilder);
               break;
            case TRUSTSTORE_REALM:
               parseTrustStoreRealm(reader, sslContextBuilder);
               hasTrustStore = true;
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      domainBuilder.setPermissionMapper((principal, roles) -> PermissionVerifier.from(new LoginPermission()));
      SecurityDomain securityDomain = domainBuilder.build();
      builder.addSecurityRealm(name, securityDomain);
      /*if (hasTrustStore) {
         sslContextBuilder.setSecurityDomain(securityDomain);
      }*/
      sslContextBuilder.setWrap(false);
      try {
         builder.addSSLContext(name, sslContextBuilder.build().create());
      } catch (GeneralSecurityException e) {
         throw new CacheConfigurationException(e);
      }
   }

   private void parseFileSystemRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      String name = "filesystem";
      String path = ParseUtils.requireAttributes(reader, Attribute.PATH)[0];
      String relativeTo = (String) reader.getProperty(Server.INFINISPAN_SERVER_DATA_PATH);
      boolean encoded = true;
      int levels = 0;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               name = value;
               break;
            case ENCODED:
               encoded = Boolean.parseBoolean(value);
               break;
            case LEVELS:
               levels = Integer.parseInt(value);
               break;
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
      FileSystemSecurityRealm fileSystemSecurityRealm = new FileSystemSecurityRealm(new File(ParseUtils.resolvePath(path, relativeTo)).toPath(), NameRewriter.IDENTITY_REWRITER, levels, encoded);
      domainBuilder.addRealm(name, fileSystemSecurityRealm).build();

   }

   private void parseTokenRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      String name = "token";
      TokenSecurityRealm.Builder tokenRealmBuilder = TokenSecurityRealm.builder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               name = value;
               break;
            case PRINCIPAL_CLAIM:
               tokenRealmBuilder.principalClaimName(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case JWT:
               parseJWT(reader, tokenRealmBuilder);
               break;
            case OAUTH2_INTROSPECTION:
               parseOauth2Introspection(reader, tokenRealmBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      domainBuilder.addRealm(name, tokenRealmBuilder.build()).build();
   }

   private void parseJWT(XMLExtendedStreamReader reader, TokenSecurityRealm.Builder tokenRealmBuilder) throws XMLStreamException {
      JwtValidator.Builder builder = JwtValidator.builder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ISSUER:
               builder.issuer(reader.getListAttributeValue(i));
               break;
            case AUDIENCE:
               builder.audience(reader.getListAttributeValue(i));
               break;
            case PUBLIC_KEY:
               builder.publicKey(value.getBytes(StandardCharsets.UTF_8));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      tokenRealmBuilder.validator(builder.build());
   }

   private void parseOauth2Introspection(XMLExtendedStreamReader reader, TokenSecurityRealm.Builder tokenRealmBuilder) throws XMLStreamException {
      OAuth2IntrospectValidator.Builder builder = OAuth2IntrospectValidator.builder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLIENT_ID:
               builder.clientId(value);
               break;
            case CLIENT_SECRET:
               builder.clientSecret(value);
               break;
            case INTROSPECTION_URL:
               try {
                  builder.tokenIntrospectionUrl(new URL(value));
               } catch (MalformedURLException e) {
                  throw new XMLStreamException(e);
               }
               break;
            case CLIENT_SSL_CONTEXT:
               break;
            case HOST_NAME_VERIFICATION_POLICY:
               builder.useSslHostnameVerifier(HostnameVerificationPolicy.valueOf(value).getVerifier());
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      tokenRealmBuilder.validator(builder.build());
   }

   private void parseKerberosRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      String keyTab = ParseUtils.requireAttributes(reader, Attribute.KEYTAB)[0];
      ParseUtils.requireNoContent(reader);
      domainBuilder.addRealm("kerberos", new KerberosSecurityRealm(new File(keyTab))).build();
   }

   private void parseLdapRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      String name = "ldap";
      SimpleDirContextFactoryBuilder dirContextBuilder = SimpleDirContextFactoryBuilder.builder();
      LdapSecurityRealmBuilder ldapRealmBuilder = LdapSecurityRealmBuilder.builder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               name = value;
               break;
            case URL:
               dirContextBuilder.setProviderUrl(value);
               break;
            case PRINCIPAL:
               dirContextBuilder.setSecurityPrincipal(value);
               break;
            case CREDENTIAL:
               dirContextBuilder.setSecurityCredential(value);
               break;
            case DIRECT_VERIFICATION:
               ldapRealmBuilder.addDirectEvidenceVerification(Boolean.parseBoolean(value));
               break;
            case PAGE_SIZE:
               ldapRealmBuilder.setPageSize(Integer.parseInt(value));
               break;
            case SEARCH_DN:
               ldapRealmBuilder.identityMapping().setSearchDn(value);
               break;
            case RDN_IDENTIFIER:
               ldapRealmBuilder.identityMapping().setRdnIdentifier(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case IDENTITY_MAPPING:
               parseLdapIdentityMapping(reader, ldapRealmBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      identityMappingBuilder.build();
      DirContextFactory dirContextFactory = dirContextBuilder.build();
      ldapRealmBuilder.setDirContextSupplier(() -> dirContextFactory.obtainDirContext(DirContextFactory.ReferralMode.FOLLOW));

      domainBuilder.addRealm(name, ldapRealmBuilder.build()).build();
      if (domainBuilder.getDefaultRealmName() == null) {
         domainBuilder.setDefaultRealmName(name);
      }
   }

   private void parseLdapIdentityMapping(XMLExtendedStreamReader reader, LdapSecurityRealmBuilder ldapRealmBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SEARCH_DN:
               ldapRealmBuilder.identityMapping().setSearchDn(value);
               break;
            case RDN_IDENTIFIER:
               ldapRealmBuilder.identityMapping().setRdnIdentifier(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ATTRIBUTE_MAPPING:
               parseLdapAttributeMapping(reader, ldapRealmBuilder);
               break;
            case USER_PASSWORD_MAPPER:
               parseLdapUserPasswordMapper(reader, ldapRealmBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseLdapUserPasswordMapper(XMLExtendedStreamReader reader, LdapSecurityRealmBuilder ldapRealmBuilder) throws XMLStreamException {
      LdapSecurityRealmBuilder.UserPasswordCredentialLoaderBuilder b = ldapRealmBuilder.userPasswordCredentialLoader();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FROM:
               b.setUserPasswordAttribute(value);
               break;
            case WRITABLE:
               if (Boolean.parseBoolean(value)) {
                  b.enablePersistence();
               }
               break;
            case VERIFIABLE:
               if (!Boolean.parseBoolean(value)) {
                  b.disableVerification();
               }
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      b.build();
   }

   private void parseLdapAttributeMapping(XMLExtendedStreamReader reader, LdapSecurityRealmBuilder ldapRealmBuilder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ATTRIBUTE:
               parseLdapAttribute(reader, ldapRealmBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseLdapAttribute(XMLExtendedStreamReader reader, LdapSecurityRealmBuilder ldapRealmBuilder) throws XMLStreamException {
      String filter = ParseUtils.requireAttributes(reader, Attribute.FILTER)[0];
      AttributeMapping.Builder attributeMappingBuilder = AttributeMapping.fromFilter(filter);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FROM:
               attributeMappingBuilder.from(value);
               break;
            case TO:
               attributeMappingBuilder.to(value);
               break;
            case FILTER:
               // Already seen
               break;
            case FILTER_BASE_DN:
               attributeMappingBuilder.searchDn(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ldapRealmBuilder.identityMapping().map(attributeMappingBuilder.build());
      ParseUtils.requireNoContent(reader);
   }

   private void parseLocalRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      String name = "local";



      ParseUtils.requireNoContent(reader);
      if (domainBuilder.getDefaultRealmName() == null) {
         domainBuilder.setDefaultRealmName(name);
      }
   }

   private void parsePropertiesRealm(XMLExtendedStreamReader reader, SecurityDomain.Builder domainBuilder) throws XMLStreamException {
      String name = "properties";
      File usersFile = null;
      File groupsFile = null;
      boolean plainText = false;
      String realmName = name;
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
      domainBuilder.addRealm(name,
            new PropertiesSecurityRealm(usersFile, groupsFile, plainText, groupsAttribute, realmName)).build();

      if (domainBuilder.getDefaultRealmName() == null) {
         domainBuilder.setDefaultRealmName(name);
      }
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
               for (String protocol : reader.getListAttributeValue(i)) {
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
         kmf.init(keyStore, keyPassword != null ? keyPassword : keyStorePassword);
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

   private void parseEndpoints(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, ServerConfigurationBuilder builder) throws XMLStreamException {
      holder.pushScope(ENDPOINTS_SCOPE);
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.SOCKET_BINDING);
      builder.applySocketBinding(attributes[0], builder.endpoint());

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SOCKET_BINDING:
               // Already seen
               break;
            default:
               parseCommonConnectorAttributes(reader, i, builder, builder.endpoint());
               break;
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         reader.handleAny(holder);
      }
      holder.popScope();
   }

   public static void parseCommonConnectorAttributes(XMLExtendedStreamReader reader, int index, ServerConfigurationBuilder serverBuilder, ProtocolServerConfigurationBuilder<?, ?> builder) throws XMLStreamException {
      String value = reader.getAttributeValue(index);
      Attribute attribute = Attribute.forName(reader.getAttributeLocalName(index));
      switch (attribute) {
         case CACHE_CONTAINER: {
            // TODO: add support for multiple containers
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
         case RECEIVE_BUFFER_SIZE: {
            builder.recvBufSize(Integer.parseInt(value));
            break;
         }
         case REQUIRE_SSL_CLIENT_AUTH: {
            builder.ssl().requireClientAuth(Boolean.parseBoolean(value));
            break;
         }
         case SECURITY_REALM: {
            builder.ssl().enable().sslContext(serverBuilder.getSSLContext(value));
            break;
         }
         case SEND_BUFFER_SIZE: {
            builder.sendBufSize(Integer.parseInt(value));
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
         default:
            throw ParseUtils.unexpectedAttribute(reader, index);
      }
   }
}
