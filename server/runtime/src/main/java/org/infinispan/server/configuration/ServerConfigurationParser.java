package org.infinispan.server.configuration;

import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;

import java.io.IOException;
import java.util.regex.Pattern;

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
import org.infinispan.server.configuration.endpoint.EndpointsConfigurationBuilder;
import org.infinispan.server.configuration.security.FileSystemRealmConfigurationBuilder;
import org.infinispan.server.configuration.security.GroupsPropertiesConfigurationBuilder;
import org.infinispan.server.configuration.security.JwtConfigurationBuilder;
import org.infinispan.server.configuration.security.KerberosSecurityFactoryConfigurationBuilder;
import org.infinispan.server.configuration.security.KeyStoreConfigurationBuilder;
import org.infinispan.server.configuration.security.LdapAttributeConfigurationBuilder;
import org.infinispan.server.configuration.security.LdapAttributeMappingConfigurationBuilder;
import org.infinispan.server.configuration.security.LdapIdentityMappingConfigurationBuilder;
import org.infinispan.server.configuration.security.LdapRealmConfigurationBuilder;
import org.infinispan.server.configuration.security.LdapUserPasswordMapperConfigurationBuilder;
import org.infinispan.server.configuration.security.LocalRealmConfigurationBuilder;
import org.infinispan.server.configuration.security.OAuth2ConfigurationBuilder;
import org.infinispan.server.configuration.security.PropertiesRealmConfigurationBuilder;
import org.infinispan.server.configuration.security.RealmConfigurationBuilder;
import org.infinispan.server.configuration.security.RealmsConfigurationBuilder;
import org.infinispan.server.configuration.security.SSLConfigurationBuilder;
import org.infinispan.server.configuration.security.SSLEngineConfigurationBuilder;
import org.infinispan.server.configuration.security.ServerIdentitiesConfigurationBuilder;
import org.infinispan.server.configuration.security.TokenRealmConfigurationBuilder;
import org.infinispan.server.configuration.security.TrustStoreRealmConfigurationBuilder;
import org.infinispan.server.configuration.security.UserPropertiesConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.auth.realm.ldap.DirContextFactory;
import org.wildfly.security.auth.util.RegexNameRewriter;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;

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
      Element element = nextElement(reader);
      if (element == Element.INTERFACES) {
         parseInterfaces(reader, builder);
         element = nextElement(reader);
      }
      if (element == Element.SOCKET_BINDINGS) {
         parseSocketBindings(reader, builder);
         element = nextElement(reader);
      }
      if (element == Element.SECURITY) {
         parseSecurity(reader, builder);
         element = nextElement(reader);
      }
      if (element == Element.DATA_SOURCES) {
         parseDataSources(reader, builder);
         element = nextElement(reader);
      }
      if (element == Element.ENDPOINTS) {
         parseEndpoints(reader, holder, builder);
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseSocketBindings(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      SocketBindingsConfigurationBuilder socketBindings = builder.socketBindings();
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.DEFAULT_INTERFACE, Attribute.PORT_OFFSET);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SOCKET_BINDING: {
               socketBindings.defaultInterface(attributes[0]).offset(Integer.parseInt(attributes[1]));
               parseSocketBinding(reader, socketBindings);
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSocketBinding(XMLExtendedStreamReader reader, SocketBindingsConfigurationBuilder builder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.PORT);
      String name = attributes[0];
      int port = Integer.parseInt(attributes[1]);
      String interfaceName = builder.defaultInterface();
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
      builder.socketBinding(name, port, interfaceName);
      ParseUtils.requireNoContent(reader);
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
      InterfaceConfigurationBuilder iface = builder.interfaces().addInterface(name);
      boolean matched = false;
      CacheConfigurationException cce = Server.log.invalidNetworkConfiguration();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         try {
            switch (element) {
               case INET_ADDRESS:
                  String value = ParseUtils.requireSingleAttribute(reader, Attribute.VALUE);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.INET_ADDRESS, value);
                  }
                  break;
               case LINK_LOCAL:
                  ParseUtils.requireNoAttributes(reader);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.LINK_LOCAL, null);
                  }
                  break;
               case GLOBAL:
                  ParseUtils.requireNoAttributes(reader);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.GLOBAL, null);
                  }
                  break;
               case LOOPBACK:
                  ParseUtils.requireNoAttributes(reader);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.LOOPBACK, null);
                  }
                  break;
               case NON_LOOPBACK:
                  ParseUtils.requireNoAttributes(reader);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.NON_LOOPBACK, null);
                  }
                  break;
               case SITE_LOCAL:
                  ParseUtils.requireNoAttributes(reader);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.SITE_LOCAL, null);
                  }
                  break;
               case MATCH_INTERFACE:
                  value = ParseUtils.requireSingleAttribute(reader, Attribute.VALUE);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.MATCH_INTERFACE, value);
                  }
                  break;
               case MATCH_ADDRESS:
                  value = ParseUtils.requireSingleAttribute(reader, Attribute.VALUE);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.MATCH_ADDRESS, value);
                  }
                  break;
               case MATCH_HOST:
                  value = ParseUtils.requireSingleAttribute(reader, Attribute.VALUE);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.MATCH_HOST, value);
                  }
                  break;
               case ANY_ADDRESS:
                  ParseUtils.requireNoAttributes(reader);
                  ParseUtils.requireNoContent(reader);
                  if (!matched) {
                     iface.address(AddressType.ANY_ADDRESS, null);
                  }
                  break;
               default:
                  throw ParseUtils.unexpectedElement(reader);
            }
            matched = true;
         } catch (IOException e) {
            cce.addSuppressed(e);
         }
      }
      if (!matched) {
         throw cce;
      }
   }

   private void parseSecurity(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      Element element = nextElement(reader);
      if (element == Element.SECURITY_REALMS) {
         parseSecurityRealms(reader, builder);
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseSecurityRealms(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      RealmsConfigurationBuilder realms = builder.security().realms();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SECURITY_REALM:
               parseSecurityRealm(reader, builder, realms);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSecurityRealm(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder, RealmsConfigurationBuilder realms) throws XMLStreamException {
      String name = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
      RealmConfigurationBuilder securityRealmBuilder = realms.addSecurityRealm(name);
      Element element = nextElement(reader);
      if (element == Element.SERVER_IDENTITIES) {
         parseServerIdentities(reader, securityRealmBuilder);
         element = nextElement(reader);
      }
      if (element == Element.FILESYSTEM_REALM) {
         parseFileSystemRealm(reader, securityRealmBuilder.fileSystemConfiguration());
         element = nextElement(reader);
      }
      if (element == Element.LDAP_REALM) {
         parseLdapRealm(reader, securityRealmBuilder.ldapConfiguration());
         element = nextElement(reader);
      }
      if (element == Element.LOCAL_REALM) {
         parseLocalRealm(reader, securityRealmBuilder.localConfiguration());
         element = nextElement(reader);
      }
      if (element == Element.PROPERTIES_REALM) {
         parsePropertiesRealm(reader, securityRealmBuilder.propertiesRealm());
         element = nextElement(reader);
      }
      if (element == Element.TOKEN_REALM) {
         parseTokenRealm(reader, builder, securityRealmBuilder.tokenConfiguration());
         element = nextElement(reader);
      }
      if (element == Element.TRUSTSTORE_REALM) {
         parseTrustStoreRealm(reader, securityRealmBuilder.trustStoreConfiguration());
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseFileSystemRealm(XMLExtendedStreamReader reader, FileSystemRealmConfigurationBuilder fileRealmBuilder) throws XMLStreamException {
      String name = "filesystem";
      String path = ParseUtils.requireAttributes(reader, Attribute.PATH)[0];
      fileRealmBuilder.path(path);
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
               fileRealmBuilder.name(name);
               break;
            case ENCODED:
               encoded = Boolean.parseBoolean(value);
               fileRealmBuilder.encoded(encoded);
               break;
            case LEVELS:
               levels = Integer.parseInt(value);
               fileRealmBuilder.levels(levels);
               break;
            case PATH:
               // Already seen
               break;
            case RELATIVE_TO:
               relativeTo = ParseUtils.requireAttributeProperty(reader, i);
               fileRealmBuilder.relativeTo(relativeTo);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      fileRealmBuilder.name(name).path(path).relativeTo(relativeTo).levels(levels).encoded(encoded).build();
   }

   private void parseTokenRealm(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, TokenRealmConfigurationBuilder tokenRealmConfigBuilder) throws XMLStreamException {
      String[] required = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.AUTH_SERVER_URL, Attribute.CLIENT_ID);
      tokenRealmConfigBuilder.name(required[0]).authServerUrl(required[1]).clientId(required[2]);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
            case AUTH_SERVER_URL:
            case CLIENT_ID:
               // Already seen
               break;
            case PRINCIPAL_CLAIM:
               tokenRealmConfigBuilder.principalClaim(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      Element element = nextElement(reader);
      if (element == Element.JWT) {
         parseJWT(reader, serverBuilder, tokenRealmConfigBuilder.jwtConfiguration());
         element = nextElement(reader);
      } else if (element == Element.OAUTH2_INTROSPECTION) {
         parseOauth2Introspection(reader, serverBuilder, tokenRealmConfigBuilder.oauth2Configuration());
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader);
      }
      tokenRealmConfigBuilder.build();
   }

   private void parseJWT(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, JwtConfigurationBuilder jwtBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ISSUER:
               jwtBuilder.issuers(reader.getListAttributeValue(i));
               break;
            case AUDIENCE:
               jwtBuilder.audience(reader.getListAttributeValue(i));
               break;
            case PUBLIC_KEY:
               jwtBuilder.publicKey(value);
               break;
            case JKU_TIMEOUT:
               jwtBuilder.jkuTimeout(Long.parseLong(value));
               break;
            case CLIENT_SSL_CONTEXT:
               jwtBuilder.clientSSLContext(value);
               break;
            case HOST_NAME_VERIFICATION_POLICY:
               jwtBuilder.hostNameVerificationPolicy(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseOauth2Introspection(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, OAuth2ConfigurationBuilder oauthBuilder) throws XMLStreamException {
      String[] required = ParseUtils.requireAttributes(reader, Attribute.CLIENT_ID, Attribute.CLIENT_SECRET, Attribute.INTROSPECTION_URL);
      oauthBuilder.clientId(required[0]).clientSecret(required[1]).introspectionUrl(required[2]);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLIENT_ID:
            case CLIENT_SECRET:
            case INTROSPECTION_URL:
               // Already seen
               break;
            case CLIENT_SSL_CONTEXT:
               oauthBuilder.clientSSLContext(value);
               break;
            case HOST_NAME_VERIFICATION_POLICY:
               oauthBuilder.hostVerificationPolicy(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseLdapRealm(XMLExtendedStreamReader reader, LdapRealmConfigurationBuilder ldapRealmConfigBuilder) throws XMLStreamException {
      ldapRealmConfigBuilder.name("ldap");
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               ldapRealmConfigBuilder.name(value);
               break;
            case URL:
               ldapRealmConfigBuilder.url(value);
               break;
            case PRINCIPAL:
               ldapRealmConfigBuilder.principal(value);
               break;
            case CREDENTIAL:
               ldapRealmConfigBuilder.credential(value);
               break;
            case DIRECT_VERIFICATION:
               ldapRealmConfigBuilder.directEvidenceVerification(Boolean.parseBoolean(value));
               break;
            case PAGE_SIZE:
               ldapRealmConfigBuilder.pageSize(Integer.parseInt(value));
               break;
            case SEARCH_DN:
               ldapRealmConfigBuilder.searchDn(value);
               break;
            case RDN_IDENTIFIER:
               ldapRealmConfigBuilder.rdnIdentifier(value);
               break;
            case CONNECTION_POOLING:
               ldapRealmConfigBuilder.connectionPooling(Boolean.parseBoolean(value));
               break;
            case CONNECTION_TIMEOUT:
               ldapRealmConfigBuilder.connectionTimeout(Integer.parseInt(value));
               break;
            case READ_TIMEOUT:
               ldapRealmConfigBuilder.readTimeout(Integer.parseInt(value));
               break;
            case REFERRAL_MODE:
               ldapRealmConfigBuilder.referralMode(DirContextFactory.ReferralMode.valueOf(value.toUpperCase()));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      Element element = nextElement(reader);
      if (element == Element.NAME_REWRITER) {
         parseNameRewriter(reader, ldapRealmConfigBuilder);
         element = nextElement(reader);
      }
      while (element == Element.IDENTITY_MAPPING) {
         parseLdapIdentityMapping(reader, ldapRealmConfigBuilder.addIdentityMap());
         element = nextElement(reader);
      }
      ldapRealmConfigBuilder.build();
   }

   private void parseNameRewriter(XMLExtendedStreamReader reader, LdapRealmConfigurationBuilder builder) throws XMLStreamException {
      Element element = nextElement(reader);
      switch (element) {
         case REGEX_PRINCIPAL_TRANSFORMER: {
            String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.PATTERN, Attribute.REPLACEMENT);
            boolean replaceAll = false;
            for (int i = 0; i < reader.getAttributeCount(); i++) {
               ParseUtils.requireNoNamespaceAttribute(reader, i);
               String value = reader.getAttributeValue(i);
               Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
               switch (attribute) {
                  case NAME:
                  case PATTERN:
                  case REPLACEMENT:
                     // Already seen
                     break;
                  case REPLACE_ALL:
                     replaceAll = Boolean.parseBoolean(value);
                     break;
                  default:
                     throw ParseUtils.unexpectedAttribute(reader, i);
               }
            }
            builder.nameRewriter(new RegexNameRewriter(Pattern.compile(attributes[1]), attributes[2], replaceAll));
            ParseUtils.requireNoContent(reader);
            break;
         }
         default:
            throw ParseUtils.unexpectedElement(reader);
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseLdapIdentityMapping(XMLExtendedStreamReader reader, LdapIdentityMappingConfigurationBuilder identityMapBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SEARCH_DN:
               identityMapBuilder.searchBaseDn(value);
               break;
            case RDN_IDENTIFIER:
               identityMapBuilder.rdnIdentifier(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      LdapUserPasswordMapperConfigurationBuilder userMapperBuilder = identityMapBuilder.addUserPasswordMapper();
      LdapAttributeMappingConfigurationBuilder attributeMapperBuilder = identityMapBuilder.addAttributeMapping();

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ATTRIBUTE_MAPPING:
               parseLdapAttributeMapping(reader, attributeMapperBuilder);
               break;
            case USER_PASSWORD_MAPPER:
               parseLdapUserPasswordMapper(reader, userMapperBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseLdapUserPasswordMapper(XMLExtendedStreamReader reader, LdapUserPasswordMapperConfigurationBuilder userMapperBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FROM:
               userMapperBuilder.from(value);
               break;
            case WRITABLE:
               boolean booleanVal = Boolean.parseBoolean(value);
               userMapperBuilder.writable(booleanVal);
               break;
            case VERIFIABLE:
               booleanVal = Boolean.parseBoolean(value);
               userMapperBuilder.verifiable(booleanVal);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      userMapperBuilder.build();
   }

   private void parseLdapAttributeMapping(XMLExtendedStreamReader reader, LdapAttributeMappingConfigurationBuilder attributeMapperBuilder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ATTRIBUTE:
               parseLdapAttributeFilter(reader, attributeMapperBuilder.addAttribute());
               break;
            case ATTRIBUTE_REFERENCE:
               parseLdapAttributeReference(reader, attributeMapperBuilder.addAttribute());
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseLdapAttributeFilter(XMLExtendedStreamReader reader, LdapAttributeConfigurationBuilder attributeBuilder) throws XMLStreamException {
      String filter = ParseUtils.requireAttributes(reader, Attribute.FILTER)[0];
      parseLdapAttribute(reader, attributeBuilder.filter(filter));
   }

   private void parseLdapAttributeReference(XMLExtendedStreamReader reader, LdapAttributeConfigurationBuilder attributeBuilder) throws XMLStreamException {
      String reference = ParseUtils.requireAttributes(reader, Attribute.REFERENCE)[0];
      parseLdapAttribute(reader, attributeBuilder.reference(reference));
   }


   private void parseLdapAttribute(XMLExtendedStreamReader reader, LdapAttributeConfigurationBuilder attributeBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case REFERENCE:
            case FILTER:
               // Already seen
               break;
            case FROM:
               attributeBuilder.from(value);
               break;
            case TO:
               attributeBuilder.to(value);
               break;
            case FILTER_DN:
               attributeBuilder.filterBaseDn(value);
               break;
            case SEARCH_RECURSIVE:
               attributeBuilder.searchRecursive(Boolean.parseBoolean(value));
               break;
            case ROLE_RECURSION:
               attributeBuilder.roleRecursion(Integer.parseInt(value));
               break;
            case ROLE_RECURSION_NAME:
               attributeBuilder.roleRecursionName(value);
               break;
            case EXTRACT_RDN:
               attributeBuilder.extractRdn(value);
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      attributeBuilder.build();
      ParseUtils.requireNoContent(reader);
   }

   private void parseLocalRealm(XMLExtendedStreamReader reader, LocalRealmConfigurationBuilder localBuilder) throws XMLStreamException {
      String name = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
      localBuilder.name(name);
      ParseUtils.requireNoContent(reader);
   }

   private void parsePropertiesRealm(XMLExtendedStreamReader reader, PropertiesRealmConfigurationBuilder propertiesBuilder) throws XMLStreamException {
      String name = "properties";
      boolean plainText = false;
      String realmName = name;
      String groupsAttribute = "groups";
      propertiesBuilder.groupAttribute(groupsAttribute);
      UserPropertiesConfigurationBuilder userPropertiesBuilder = propertiesBuilder.userProperties();
      GroupsPropertiesConfigurationBuilder groupsBuilder = propertiesBuilder.groupProperties();
      userPropertiesBuilder.digestRealmName(name).plainText(false);
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
      propertiesBuilder.groupAttribute(groupsAttribute);
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
         userPropertiesBuilder.path(path).relativeTo(relativeTo).plainText(plainText).digestRealmName(realmName);
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
         groupsBuilder.path(path).relativeTo(relativeTo);
         ParseUtils.requireNoContent(reader);
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }

      propertiesBuilder.build();
   }

   private void parseServerIdentities(XMLExtendedStreamReader reader, RealmConfigurationBuilder securityRealmBuilder) throws
         XMLStreamException {
      ServerIdentitiesConfigurationBuilder identitiesBuilder = securityRealmBuilder.serverIdentitiesConfiguration();
      Element element = nextElement(reader);
      if (element == Element.SSL) {
         parseSSL(reader, identitiesBuilder);
         element = nextElement(reader);
      }
      while (element == Element.KERBEROS) {
         parseKerberos(reader, identitiesBuilder);
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseSSL(XMLExtendedStreamReader reader, ServerIdentitiesConfigurationBuilder identitiesBuilder) throws
         XMLStreamException {
      SSLConfigurationBuilder serverIdentitiesBuilder = identitiesBuilder.addSslConfiguration();
      Element element = nextElement(reader);
      if (element == Element.KEYSTORE) {
         parseKeyStore(reader, serverIdentitiesBuilder.keyStore());
         element = nextElement(reader);
      }
      if (element == Element.ENGINE) {
         parseSSLEngine(reader, serverIdentitiesBuilder.engine());
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseSSLEngine(XMLExtendedStreamReader reader, SSLEngineConfigurationBuilder engine) throws
         XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED_PROTOCOLS:
               engine.enabledProtocols(reader.getListAttributeValue(i));
               break;
            case ENABLED_CIPHERSUITES:
               engine.enabledCiphersuites(reader.getAttributeValue(i));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseKeyStore(XMLExtendedStreamReader reader, KeyStoreConfigurationBuilder keyStoreBuilder) throws
         XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.PATH);
      keyStoreBuilder.path(attributes[0]);
      String relativeTo = (String) reader.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH);
      keyStoreBuilder.relativeTo(relativeTo);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PATH:
               // Already seen
               break;
            case PROVIDER:
               keyStoreBuilder.provider(value);
               break;
            case RELATIVE_TO:
               relativeTo = ParseUtils.requireAttributeProperty(reader, i);
               keyStoreBuilder.relativeTo(relativeTo);
               break;
            case KEYSTORE_PASSWORD:
               keyStoreBuilder.keyStorePassword(value.toCharArray());
               break;
            case ALIAS:
               keyStoreBuilder.alias(value);
               break;
            case KEY_PASSWORD:
               keyStoreBuilder.keyPassword(value.toCharArray());
               break;
            case GENERATE_SELF_SIGNED_CERTIFICATE_HOST:
               keyStoreBuilder.generateSelfSignedCertificateHost(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      keyStoreBuilder.build();
   }

   private void parseTrustStoreRealm(XMLExtendedStreamReader reader, TrustStoreRealmConfigurationBuilder trustStoreBuilder) throws
         XMLStreamException {
      String name = "trust";
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
            case NAME:
               name = value;
               break;
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
      trustStoreBuilder.name(name).path(path).relativeTo(relativeTo).keyStorePassword(keyStorePassword).provider(keyStoreProvider);
      ParseUtils.requireNoContent(reader);
      trustStoreBuilder.build();
   }

   private void parseKerberos(XMLExtendedStreamReader reader, ServerIdentitiesConfigurationBuilder identitiesBuilder) throws XMLStreamException {
      KerberosSecurityFactoryConfigurationBuilder builder = identitiesBuilder.addKerberosConfiguration();
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.KEYTAB_PATH, Attribute.PRINCIPAL);
      builder.keyTabPath(attributes[0]).relativeTo((String) reader.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH));
      builder.principal(attributes[1]);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case KEYTAB_PATH:
            case PRINCIPAL:
               // Already seen, ignore
               break;
            case RELATIVE_TO:
               builder.relativeTo(ParseUtils.requireAttributeProperty(reader, i));
               break;
            case DEBUG:
               builder.debug(Boolean.parseBoolean(value));
               break;
            case FAIL_CACHE:
               builder.failCache(Long.parseLong(value));
               break;
            case MECHANISM_NAMES:
               for (String name : ParseUtils.getListAttributeValue(value)) {
                  builder.addMechanismName(name);
               }
               break;
            case MECHANISM_OIDS:
               for (String oid : ParseUtils.getListAttributeValue(value)) {
                  builder.addMechanismOid(oid);
               }
               break;
            case MINIMUM_REMAINING_LIFETIME:
               builder.minimumRemainingLifetime(Integer.parseInt(value));
               break;
            case OBTAIN_KERBEROS_TICKET:
               builder.obtainKerberosTicket(Boolean.parseBoolean(value));
               break;
            case REQUEST_LIFETIME:
               builder.requestLifetime(Integer.parseInt(value));
               break;
            case REQUIRED:
               builder.checkKeyTab(Boolean.parseBoolean(value));
               break;
            case SERVER:
               builder.server(Boolean.parseBoolean(value));
               break;
            case WRAP_GSS_CREDENTIAL:
               builder.wrapGssCredential(Boolean.parseBoolean(value));
               break;

            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      // Add all options
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case OPTION:
               String[] option = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.VALUE);
               builder.addOption(option[0], option[1]);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseDataSources(XMLExtendedStreamReader reader, ServerConfigurationBuilder builder) throws XMLStreamException {
      DataSourcesConfigurationBuilder dataSources = builder.dataSources();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case DATA_SOURCE: {
               parseDataSource(reader, dataSources);
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseDataSource(XMLExtendedStreamReader reader, DataSourcesConfigurationBuilder dataSourcesBuilder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.JNDI_NAME);
      String name = attributes[0];
      String jndiName = attributes[1];
      DataSourceConfigurationBuilder dataSourceBuilder = dataSourcesBuilder.dataSource(name, jndiName);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case JNDI_NAME:
            case NAME:
               // already parsed
               break;
            case STATISTICS:
               dataSourceBuilder.statistics(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      Element element = nextElement(reader);
      if (element != Element.CONNECTION_FACTORY) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
      parseDataSourceConnectionFactory(reader, dataSourceBuilder);
      element = nextElement(reader);
      if (element != Element.CONNECTION_POOL) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
      parseDataSourceConnectionPool(reader, dataSourceBuilder);
      element = nextElement(reader);
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseDataSourceConnectionFactory(XMLExtendedStreamReader reader, DataSourceConfigurationBuilder dataSourceBuilder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.DRIVER);
      dataSourceBuilder.driver(attributes[0]);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case DRIVER:
               // already parsed
               break;
            case USERNAME:
               dataSourceBuilder.username(value);
               break;
            case PASSWORD:
               dataSourceBuilder.password(value);
               break;
            case URL:
               dataSourceBuilder.url(value);
               break;
            case TRANSACTION_ISOLATION:
               dataSourceBuilder.transactionIsolation(AgroalConnectionFactoryConfiguration.TransactionIsolation.valueOf(value));
               break;
            case NEW_CONNECTION_SQL:
               dataSourceBuilder.newConnectionSql(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CONNECTION_PROPERTY: {
               dataSourceBuilder.addProperty(ParseUtils.requireSingleAttribute(reader, Attribute.NAME), reader.getElementText());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseDataSourceConnectionPool(XMLExtendedStreamReader reader, DataSourceConfigurationBuilder dataSourceBuilder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.MAX_SIZE);
      dataSourceBuilder.maxSize(Integer.parseInt(attributes[0]));
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case MAX_SIZE:
               // already parsed
               break;
            case MIN_SIZE:
               dataSourceBuilder.minSize(Integer.parseInt(value));
               break;
            case INITIAL_SIZE:
               dataSourceBuilder.initialSize(Integer.parseInt(value));
               break;
            case BLOCKING_TIMEOUT:
               dataSourceBuilder.blockingTimeout(Integer.parseInt(value));
               break;
            case BACKGROUND_VALIDATION:
               dataSourceBuilder.backgroundValidation(Long.parseLong(value));
               break;
            case LEAK_DETECTION:
               dataSourceBuilder.leakDetection(Long.parseLong(value));
               break;
            case IDLE_REMOVAL:
               dataSourceBuilder.idleRemoval(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseEndpoints(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, ServerConfigurationBuilder builder) throws XMLStreamException {
      EndpointsConfigurationBuilder endpoints = builder.endpoints();
      holder.pushScope(ENDPOINTS_SCOPE);
      String socketBinding = ParseUtils.requireAttributes(reader, Attribute.SOCKET_BINDING)[0];
      endpoints.socketBinding(socketBinding);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         String value = reader.getAttributeValue(i);
         switch (attribute) {
            case SOCKET_BINDING:
               // Already seen
               break;
            case SECURITY_REALM:
               // Set the endpoint security realm and fall-through. Starting with 11.0 we also enable implicit authentication configuration
               endpoints.securityRealm(value).implicitConnectorSecurity(reader.getSchema().since(11, 0));
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
      if (ParseUtils.isNoNamespaceAttribute(reader, index)) {
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(index));
         String value = reader.getAttributeValue(index);
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
               if (serverBuilder.hasSSLContext(value)) {
                  builder.ssl().enable().sslContext(serverBuilder.getSSLContext(value));
               }
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
}
