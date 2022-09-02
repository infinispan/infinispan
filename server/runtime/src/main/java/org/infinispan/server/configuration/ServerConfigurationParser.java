package org.infinispan.server.configuration;

import static org.infinispan.configuration.parsing.ParseUtils.ignoreAttribute;
import static org.infinispan.util.logging.Log.CONFIG;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.endpoint.EndpointConfigurationBuilder;
import org.infinispan.server.configuration.security.CredentialStoreConfiguration;
import org.infinispan.server.configuration.security.CredentialStoreConfigurationBuilder;
import org.infinispan.server.configuration.security.CredentialStoresConfigurationBuilder;
import org.infinispan.server.configuration.security.DistributedRealmConfigurationBuilder;
import org.infinispan.server.configuration.security.GroupsPropertiesConfigurationBuilder;
import org.infinispan.server.configuration.security.JwtConfigurationBuilder;
import org.infinispan.server.configuration.security.KerberosSecurityFactoryConfigurationBuilder;
import org.infinispan.server.configuration.security.KeyStoreConfigurationBuilder;
import org.infinispan.server.configuration.security.LdapAttributeConfigurationBuilder;
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
import org.infinispan.server.configuration.security.TrustStoreConfigurationBuilder;
import org.infinispan.server.configuration.security.TrustStoreRealmConfigurationBuilder;
import org.infinispan.server.configuration.security.UserPropertiesConfigurationBuilder;
import org.infinispan.server.core.configuration.IpFilterConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.security.PasswordCredentialSource;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.auth.realm.ldap.DirContextFactory;
import org.wildfly.security.auth.util.RegexNameRewriter;
import org.wildfly.security.credential.source.CredentialSource;

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
      @Namespace(uri = "urn:infinispan:server:*", root = "transport"),
})
public class ServerConfigurationParser implements ConfigurationParser {
   private static final org.infinispan.util.logging.Log coreLog = org.infinispan.util.logging.LogFactory.getLog(ServerConfigurationParser.class);
   static final String NAMESPACE = "urn:infinispan:server:";
   public static final EnumSet<Element> CREDENTIAL_TYPES = EnumSet.of(Element.CREDENTIAL_REFERENCE, Element.CLEAR_TEXT_CREDENTIAL, Element.MASKED_CREDENTIAL, Element.COMMAND_CREDENTIAL);
   public static String ENDPOINTS_SCOPE = "ENDPOINTS";

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   public static Element nextElement(ConfigurationReader reader) {
      if (reader.nextElement() == ConfigurationReader.ElementType.END_ELEMENT) {
         return null;
      }
      return Element.forName(reader.getLocalName());
   }

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      if (!holder.inScope(ParserScope.GLOBAL)) {
         throw coreLog.invalidScope(ParserScope.GLOBAL.name(), holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case SERVER: {
            builder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
            ServerConfigurationBuilder serverConfigurationBuilder = builder.addModule(ServerConfigurationBuilder.class).properties(reader.getProperties());
            parseServerElements(reader, holder, serverConfigurationBuilder);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseServerElements(ConfigurationReader reader, ConfigurationBuilderHolder holder, ServerConfigurationBuilder builder) {
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case INTERFACES:
               parseInterfaces(reader, builder);
               break;
            case SOCKET_BINDINGS:
               parseSocketBindings(reader, builder);
               break;
            case SECURITY:
               parseSecurity(reader, builder);
               break;
            case DATA_SOURCES:
               parseDataSources(reader, builder);
               break;
            case ENDPOINTS:
               holder.pushScope(ENDPOINTS_SCOPE);
               if (reader.getSchema().since(13, 0)) {
                  parseEndpoints(reader, holder, builder);
               } else {
                  while (reader.inTag()) {
                     parseEndpoint(reader, holder, builder, Element.ENDPOINTS, null, null);
                  }
               }
               holder.popScope();
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
   }

   private void parseSocketBindings(ConfigurationReader reader, ServerConfigurationBuilder builder) {
      SocketBindingsConfigurationBuilder socketBindings = builder.socketBindings();
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.DEFAULT_INTERFACE, Attribute.PORT_OFFSET);
      while (reader.inTag(Element.SOCKET_BINDINGS)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case BINDINGS: {
               break;
            }
            case SOCKET_BINDING: {
               if (reader.getAttributeCount() > 0) {
                  socketBindings.defaultInterface(attributes[0]).offset(Integer.parseInt(attributes[1]));
                  parseSocketBinding(reader, socketBindings);
               }
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSocketBinding(ConfigurationReader reader, SocketBindingsConfigurationBuilder builder) {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.PORT);
      String name = attributes[0];
      int port = Integer.parseInt(attributes[1]);
      String interfaceName = builder.defaultInterface();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

   private void parseInterfaces(ConfigurationReader reader, ServerConfigurationBuilder builder) {
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case INTERFACES: // JSON/YAML array
            case INTERFACE:
               parseInterface(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseInterface(ConfigurationReader reader, ServerConfigurationBuilder builder) {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME);
      String name = attributes[0];
      InterfaceConfigurationBuilder iface = builder.interfaces().addInterface(name);
      boolean matched = false;
      CacheConfigurationException cce = Server.log.invalidNetworkConfiguration();
      while (reader.inTag()) {
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

   private void parseSecurity(ConfigurationReader reader, ServerConfigurationBuilder builder) {
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CREDENTIAL_STORES:
               parseCredentialStores(reader, builder);
               break;
            case SECURITY_REALMS:
               parseSecurityRealms(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
   }

   private void parseCredentialStores(ConfigurationReader reader, ServerConfigurationBuilder builder) {
      CredentialStoresConfigurationBuilder credentialStores = builder.security().credentialStores();
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CREDENTIAL_STORE:
            case CREDENTIAL_STORES:
               parseCredentialStore(reader, builder, credentialStores);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseCredentialStore(ConfigurationReader reader, ServerConfigurationBuilder builder, CredentialStoresConfigurationBuilder credentialStores) {
      String name = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.PATH)[0];
      CredentialStoreConfigurationBuilder credentialStoreBuilder = credentialStores.addCredentialStore(name);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME:
               // Already seen.
               break;
            case PATH:
               credentialStoreBuilder.path(value);
               break;
            case RELATIVE_TO:
               credentialStoreBuilder.relativeTo(value);
               break;
            case TYPE:
               credentialStoreBuilder.type(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      Element element = nextElement(reader);
      if (CREDENTIAL_TYPES.contains(element)) {
         credentialStoreBuilder.credential(parseCredentialReference(reader, builder));
         element = nextElement(reader);
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private Supplier<CredentialSource> parseCredentialReference(ConfigurationReader reader, ServerConfigurationBuilder builder) {
      switch (Element.forName(reader.getLocalName())) {
         case CREDENTIAL_REFERENCE: {
            String store = null;
            String alias = null;
            for (int i = 0; i < reader.getAttributeCount(); i++) {
               ParseUtils.requireNoNamespaceAttribute(reader, i);
               String value = reader.getAttributeValue(i);
               Attribute attribute = Attribute.forName(reader.getAttributeName(i));
               switch (attribute) {
                  case STORE:
                     store = value;
                     break;
                  case ALIAS:
                     alias = value;
                     break;
                  default:
                     throw ParseUtils.unexpectedAttribute(reader, i);
               }
            }
            ParseUtils.requireNoContent(reader);
            return builder.security().credentialStores().getCredential(store, alias);
         }
         case CLEAR_TEXT_CREDENTIAL: {
            String credential = ParseUtils.requireSingleAttribute(reader, Attribute.CLEAR_TEXT);
            ParseUtils.requireNoContent(reader);
            return new CredentialStoreConfiguration.ClearTextCredentialSupplier(credential.toCharArray());
         }
         case MASKED_CREDENTIAL: {
            String masked = ParseUtils.requireSingleAttribute(reader, Attribute.MASKED);
            ParseUtils.requireNoContent(reader);
            return new CredentialStoreConfiguration.MaskedCredentialSupplier(masked);
         }
         case COMMAND_CREDENTIAL: {
            String command = ParseUtils.requireSingleAttribute(reader, Attribute.COMMAND);
            ParseUtils.requireNoContent(reader);
            return new CredentialStoreConfiguration.CommandCredentialSupplier(command);
         }
         default:
            throw ParseUtils.unexpectedElement(reader);
      }
   }

   private void parseSecurityRealms(ConfigurationReader reader, ServerConfigurationBuilder builder) {
      RealmsConfigurationBuilder realms = builder.security().realms();
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SECURITY_REALM:
            case SECURITY_REALMS:
               parseSecurityRealm(reader, builder, realms);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSecurityRealm(ConfigurationReader reader, ServerConfigurationBuilder builder, RealmsConfigurationBuilder realms) {
      String name = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
      RealmConfigurationBuilder securityRealmBuilder = realms.addSecurityRealm(name);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME:
               // Already seen.
               break;
            case DEFAULT_REALM:
               securityRealmBuilder.defaultRealm(value);
               break;
            case CACHE_LIFESPAN:
               securityRealmBuilder.cacheLifespan(Long.valueOf(value));
               break;
            case CACHE_MAX_SIZE:
               securityRealmBuilder.cacheMaxSize(Integer.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SERVER_IDENTITIES:
               parseServerIdentities(reader, builder, securityRealmBuilder);
               break;
            case DISTRIBUTED_REALM:
               parseDistributedRealm(reader, securityRealmBuilder.distributedConfiguration());
               break;
            case LDAP_REALM:
               parseLdapRealm(reader, builder, securityRealmBuilder.ldapConfiguration());
               break;
            case LOCAL_REALM:
               parseLocalRealm(reader, securityRealmBuilder.localConfiguration());
               break;
            case PROPERTIES_REALM:
               parsePropertiesRealm(reader, securityRealmBuilder.propertiesConfiguration(), name);
               break;
            case TOKEN_REALM:
               parseTokenRealm(reader, builder, securityRealmBuilder.tokenConfiguration());
               break;
            case TRUSTSTORE_REALM:
               if (reader.getSchema().since(12, 1)) {
                  parseTrustStoreRealm(reader, securityRealmBuilder.trustStoreConfiguration());
               } else {
                  parseLegacyTrustStoreRealm(reader, builder, securityRealmBuilder.trustStoreConfiguration(), securityRealmBuilder.serverIdentitiesConfiguration());
               }
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
   }

   private void parseTokenRealm(ConfigurationReader reader, ServerConfigurationBuilder serverBuilder, TokenRealmConfigurationBuilder tokenRealmConfigBuilder) {
      String[] required = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.AUTH_SERVER_URL, Attribute.CLIENT_ID);
      tokenRealmConfigBuilder.name(required[0]).authServerUrl(required[1]).clientId(required[2]);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
   }

   private void parseJWT(ConfigurationReader reader, ServerConfigurationBuilder serverBuilder, JwtConfigurationBuilder jwtBuilder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
            case CONNECTION_TIMEOUT:
               jwtBuilder.connectionTimeout(Integer.parseInt(value));
               break;
            case READ_TIMEOUT:
               jwtBuilder.readTimeout(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseOauth2Introspection(ConfigurationReader reader, ServerConfigurationBuilder serverBuilder, OAuth2ConfigurationBuilder oauthBuilder) {
      String[] required = ParseUtils.requireAttributes(reader, Attribute.CLIENT_ID, Attribute.INTROSPECTION_URL);
      oauthBuilder.clientId(required[0]).introspectionUrl(required[1]);
      boolean credentialSet = false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case CLIENT_ID:
            case INTROSPECTION_URL:
               // Already seen
               break;
            case CLIENT_SECRET:
               oauthBuilder.clientSecret(value.toCharArray());
               credentialSet = true;
               break;
            case CLIENT_SSL_CONTEXT:
               oauthBuilder.clientSSLContext(value);
               break;
            case HOST_NAME_VERIFICATION_POLICY:
               oauthBuilder.hostVerificationPolicy(value);
               break;
            case CONNECTION_TIMEOUT:
               oauthBuilder.connectionTimeout(Integer.parseInt(value));
               break;
            case READ_TIMEOUT:
               oauthBuilder.readTimeout(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      Element element = nextElement(reader);
      if (element == Element.CREDENTIAL_REFERENCE) {
         if (credentialSet) {
            throw Server.log.cannotOverrideCredential(Element.OAUTH2_INTROSPECTION.toString(), Attribute.CLIENT_SECRET.toString());
         }
         oauthBuilder.clientSecret(parseCredentialReference(reader, serverBuilder));
         credentialSet = true;
         element = nextElement(reader);
      }
      if (!credentialSet) {
         throw Server.log.missingCredential(Element.OAUTH2_INTROSPECTION.toString(), Attribute.CLIENT_SECRET.toString());
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseLdapRealm(ConfigurationReader reader, ServerConfigurationBuilder builder, LdapRealmConfigurationBuilder ldapRealmConfigBuilder) {
      boolean credentialSet = false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
               ldapRealmConfigBuilder.credential(value.toCharArray());
               credentialSet = true;
               break;
            case DIRECT_VERIFICATION:
               ldapRealmConfigBuilder.directEvidenceVerification(Boolean.parseBoolean(value));
               break;
            case PAGE_SIZE:
               ldapRealmConfigBuilder.pageSize(Integer.parseInt(value));
               break;
            case SEARCH_DN:
               if (reader.getSchema().since(13, 0)) {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               } else {
                  ldapRealmConfigBuilder.identityMapping().searchBaseDn(value);
               }
               break;
            case RDN_IDENTIFIER:
               if (reader.getSchema().since(13, 0)) {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               } else {
                  ldapRealmConfigBuilder.identityMapping().rdnIdentifier(value);
               }
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
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CREDENTIAL_REFERENCE:
               if (credentialSet) {
                  throw Server.log.cannotOverrideCredential(Element.LDAP_REALM.toString(), Attribute.CREDENTIAL.toString());
               }
               ldapRealmConfigBuilder.credential(parseCredentialReference(reader, builder));
               credentialSet = true;
               break;
            case NAME_REWRITER:
               parseNameRewriter(reader, ldapRealmConfigBuilder);
               break;
            case IDENTITY_MAPPING:
               parseLdapIdentityMapping(reader, ldapRealmConfigBuilder.identityMapping());
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
      if (!credentialSet) {
         throw Server.log.missingCredential(Element.LDAP_REALM.toString(), Attribute.CREDENTIAL.toString());
      }
   }

   private void parseNameRewriter(ConfigurationReader reader, LdapRealmConfigurationBuilder builder) {
      Element element = nextElement(reader);
      switch (element) {
         case REGEX_PRINCIPAL_TRANSFORMER: {
            String[] attributes = ParseUtils.requireAttributes(reader, Attribute.PATTERN, Attribute.REPLACEMENT);
            boolean replaceAll = false;
            for (int i = 0; i < reader.getAttributeCount(); i++) {
               ParseUtils.requireNoNamespaceAttribute(reader, i);
               String value = reader.getAttributeValue(i);
               Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
            builder.nameRewriter(new RegexNameRewriter(Pattern.compile(attributes[0]), attributes[1], replaceAll));
            ParseUtils.requireNoContent(reader);
            break;
         }
         default:
            throw ParseUtils.unexpectedElement(reader);
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseLdapIdentityMapping(ConfigurationReader reader, LdapIdentityMappingConfigurationBuilder identityMapBuilder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case SEARCH_DN:
               identityMapBuilder.searchBaseDn(value);
               break;
            case RDN_IDENTIFIER:
               identityMapBuilder.rdnIdentifier(value);
               break;
            case SEARCH_RECURSIVE:
               identityMapBuilder.searchRecursive(Boolean.valueOf(value));
               break;
            case SEARCH_TIME_LIMIT:
               identityMapBuilder.searchTimeLimit(Integer.parseInt(value));
               break;
            case FILTER_NAME:
               identityMapBuilder.filterName(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ATTRIBUTE_MAPPING:
               parseLdapAttributeMapping(reader, identityMapBuilder);
               break;
            case USER_PASSWORD_MAPPER:
               parseLdapUserPasswordMapper(reader, identityMapBuilder.userPasswordMapper());
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
   }

   private void parseLdapUserPasswordMapper(ConfigurationReader reader, LdapUserPasswordMapperConfigurationBuilder userMapperBuilder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case FROM:
               userMapperBuilder.from(value);
               break;
            case WRITABLE:
               if (reader.getSchema().since(13, 0)) {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               } else {
                  ignoreAttribute(reader, i);
               }
               break;
            case VERIFIABLE:
               userMapperBuilder.verifiable(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseLdapAttributeMapping(ConfigurationReader reader, LdapIdentityMappingConfigurationBuilder identityMappingConfigurationBuilder) {
      ParseUtils.requireNoAttributes(reader);
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ATTRIBUTE:
               parseLdapAttributeFilter(reader, identityMappingConfigurationBuilder.addAttributeMapping());
               break;
            case ATTRIBUTE_REFERENCE:
               parseLdapAttributeReference(reader, identityMappingConfigurationBuilder.addAttributeMapping());
               break;
            case ATTRIBUTE_MAPPING:
               // JSON mode, determine the type from the attribute
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  Attribute attribute = Attribute.forName(reader.getAttributeName(i));
                  switch (attribute) {
                     case FILTER:
                        parseLdapAttributeFilter(reader, identityMappingConfigurationBuilder.addAttributeMapping());
                        break;
                     case REFERENCE:
                        parseLdapAttributeReference(reader, identityMappingConfigurationBuilder.addAttributeMapping());
                        break;
                  }
               }
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseLdapAttributeFilter(ConfigurationReader reader, LdapAttributeConfigurationBuilder attributeBuilder) {
      String filter = ParseUtils.requireAttributes(reader, Attribute.FILTER)[0];
      parseLdapAttribute(reader, attributeBuilder.filter(filter));
   }

   private void parseLdapAttributeReference(ConfigurationReader reader, LdapAttributeConfigurationBuilder attributeBuilder) {
      String reference = ParseUtils.requireAttributes(reader, Attribute.REFERENCE)[0];
      parseLdapAttribute(reader, attributeBuilder.reference(reference));
   }

   private void parseLdapAttribute(ConfigurationReader reader, LdapAttributeConfigurationBuilder attributeBuilder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
      ParseUtils.requireNoContent(reader);
   }

   private void parseLocalRealm(ConfigurationReader reader, LocalRealmConfigurationBuilder localBuilder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME:
               localBuilder.name(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parsePropertiesRealm(ConfigurationReader reader, PropertiesRealmConfigurationBuilder propertiesBuilder, String realmName) {
      UserPropertiesConfigurationBuilder userPropertiesBuilder = propertiesBuilder.userProperties().digestRealmName(realmName);
      GroupsPropertiesConfigurationBuilder groupsBuilder = propertiesBuilder.groupProperties();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME:
               propertiesBuilder.name(value);
               userPropertiesBuilder.digestRealmName(value);
               break;
            case GROUPS_ATTRIBUTE:
               propertiesBuilder.groupAttribute(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case USER_PROPERTIES: {
               String path = ParseUtils.requireAttributes(reader, Attribute.PATH)[0];
               String relativeTo = Server.INFINISPAN_SERVER_CONFIG_PATH;
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  ParseUtils.requireNoNamespaceAttribute(reader, i);
                  String value = reader.getAttributeValue(i);
                  Attribute attribute = Attribute.forName(reader.getAttributeName(i));
                  switch (attribute) {
                     case PATH:
                        // Already seen
                        break;
                     case RELATIVE_TO:
                        relativeTo = value;
                        break;
                     case DIGEST_REALM_NAME:
                        userPropertiesBuilder.digestRealmName(value);
                        break;
                     case PLAIN_TEXT:
                        userPropertiesBuilder.plainText(Boolean.parseBoolean(value));
                        break;
                     default:
                        throw ParseUtils.unexpectedAttribute(reader, i);
                  }
               }
               userPropertiesBuilder.path(path).relativeTo(relativeTo);
               ParseUtils.requireNoContent(reader);
               break;
            }
            case GROUP_PROPERTIES: {
               String path = ParseUtils.requireAttributes(reader, Attribute.PATH)[0];
               String relativeTo = Server.INFINISPAN_SERVER_CONFIG_PATH;
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  ParseUtils.requireNoNamespaceAttribute(reader, i);
                  Attribute attribute = Attribute.forName(reader.getAttributeName(i));
                  switch (attribute) {
                     case PATH:
                        // Already seen
                        break;
                     case RELATIVE_TO:
                        relativeTo = reader.getAttributeValue(i);
                        break;
                     default:
                        throw ParseUtils.unexpectedAttribute(reader, i);
                  }
               }
               groupsBuilder.path(path).relativeTo(relativeTo);
               ParseUtils.requireNoContent(reader);
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
   }

   private void parseDistributedRealm(ConfigurationReader reader, DistributedRealmConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME:
               builder.name(value);
               break;
            case REALMS:
               ParseUtils.requireAttributes(reader, Attribute.REALMS)[0].split("\\s+");
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseServerIdentities(ConfigurationReader reader, ServerConfigurationBuilder builder, RealmConfigurationBuilder securityRealmBuilder) {
      ServerIdentitiesConfigurationBuilder identitiesBuilder = securityRealmBuilder.serverIdentitiesConfiguration();
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SSL:
               parseSSL(reader, builder, identitiesBuilder);
               break;
            case KERBEROS:
               parseKerberos(reader, identitiesBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
   }

   private void parseSSL(ConfigurationReader reader, ServerConfigurationBuilder builder, ServerIdentitiesConfigurationBuilder identitiesBuilder) {
      SSLConfigurationBuilder serverIdentitiesBuilder = identitiesBuilder.sslConfiguration();
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case KEYSTORE:
               parseKeyStore(reader, builder, serverIdentitiesBuilder.keyStore());
               break;
            case TRUSTSTORE:
               parseTrustStore(reader, builder, serverIdentitiesBuilder.trustStore());
               break;
            case ENGINE:
               parseSSLEngine(reader, serverIdentitiesBuilder.engine());
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
   }

   private void parseSSLEngine(ConfigurationReader reader, SSLEngineConfigurationBuilder engine) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case ENABLED_PROTOCOLS:
               engine.enabledProtocols(reader.getListAttributeValue(i));
               break;
            case ENABLED_CIPHERSUITES:
               engine.enabledCiphersuitesFilter(reader.getAttributeValue(i));
               break;
            case ENABLED_CIPHERSUITES_TLS13:
               engine.enabledCiphersuitesNames(reader.getAttributeValue(i));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseKeyStore(ConfigurationReader reader, ServerConfigurationBuilder builder, KeyStoreConfigurationBuilder keyStoreBuilder) {
      boolean credentialSet = false, pathSet = false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case PATH:
               keyStoreBuilder.path(value);
               pathSet = true;
               break;
            case PROVIDER:
               keyStoreBuilder.provider(value);
               break;
            case TYPE:
               keyStoreBuilder.type(value);
               break;
            case RELATIVE_TO:
               keyStoreBuilder.relativeTo(value);
               break;
            case KEYSTORE_PASSWORD:
               CONFIG.attributeDeprecatedUseOther(Attribute.KEYSTORE_PASSWORD, Element.KEYSTORE, Attribute.PASSWORD);
            case PASSWORD:
               keyStoreBuilder.keyStorePassword(value.toCharArray());
               credentialSet = true;
               break;
            case ALIAS:
               keyStoreBuilder.alias(value);
               break;
            case KEY_PASSWORD:
               CONFIG.configDeprecated(Attribute.KEY_PASSWORD);
               keyStoreBuilder.keyPassword(value.toCharArray());
               break;
            case GENERATE_SELF_SIGNED_CERTIFICATE_HOST:
               keyStoreBuilder.generateSelfSignedCertificateHost(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      Element element = nextElement(reader);
      if (element == Element.CREDENTIAL_REFERENCE) {
         if (credentialSet) {
            throw Server.log.cannotOverrideCredential(Element.KEYSTORE.toString(), Attribute.KEYSTORE_PASSWORD.toString());
         }
         keyStoreBuilder.keyStorePassword(parseCredentialReference(reader, builder));
         credentialSet = true;
         element = nextElement(reader);
      }
      if (!credentialSet && pathSet) {
         throw Server.log.missingCredential(Element.KEYSTORE.toString(), Attribute.KEYSTORE_PASSWORD.toString());
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseTrustStore(ConfigurationReader reader, ServerConfigurationBuilder builder, TrustStoreConfigurationBuilder trustStoreBuilder) {
      boolean credentialSet = false, pathSet = false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case PATH:
               trustStoreBuilder.path(value);
               pathSet = true;
               break;
            case PROVIDER:
               trustStoreBuilder.provider(value);
               break;
            case TYPE:
               trustStoreBuilder.type(value);
               break;
            case RELATIVE_TO:
               trustStoreBuilder.relativeTo(value);
               break;
            case PASSWORD:
               trustStoreBuilder.password(value.toCharArray());
               credentialSet = true;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      Element element = nextElement(reader);
      if (element == Element.CREDENTIAL_REFERENCE) {
         if (credentialSet) {
            throw Server.log.cannotOverrideCredential(Element.TRUSTSTORE.toString(), Attribute.PASSWORD.toString());
         }
         trustStoreBuilder.password(parseCredentialReference(reader, builder));
         credentialSet = true;
         element = nextElement(reader);
      }
      if (!credentialSet && pathSet) {
         throw Server.log.missingCredential(Element.TRUSTSTORE.toString(), Attribute.PASSWORD.toString());
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseTrustStoreRealm(ConfigurationReader reader, TrustStoreRealmConfigurationBuilder trustStoreBuilder) {
      String name = "trust";
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME:
               name = value;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
      trustStoreBuilder.name(name);
   }

   private void parseLegacyTrustStoreRealm(ConfigurationReader reader, ServerConfigurationBuilder builder, TrustStoreRealmConfigurationBuilder trustStoreBuilder, ServerIdentitiesConfigurationBuilder serverIdentitiesConfigurationBuilder) {
      String name = "trust";
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.PATH);
      String path = attributes[0];
      String relativeTo = (String) reader.getProperty(Server.INFINISPAN_SERVER_CONFIG_PATH);
      String keyStoreProvider = null;
      Supplier<CredentialSource> keyStorePassword = null;
      boolean credentialSet = false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
               keyStorePassword = new PasswordCredentialSource(value.toCharArray());
               credentialSet = true;
               break;
            case RELATIVE_TO:
               relativeTo = ParseUtils.requireAttributeProperty(reader, i);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      Element element = nextElement(reader);
      if (element == Element.CREDENTIAL_REFERENCE) {
         if (credentialSet) {
            throw Server.log.cannotOverrideCredential(Element.TRUSTSTORE_REALM.toString(), Attribute.KEYSTORE_PASSWORD.toString());
         }
         keyStorePassword = parseCredentialReference(reader, builder);
         credentialSet = true;
         element = nextElement(reader);
      }
      if (!credentialSet) {
         throw Server.log.missingCredential(Element.TRUSTSTORE_REALM.toString(), Attribute.KEYSTORE_PASSWORD.toString());
      }
      if (element != null) {
         throw ParseUtils.unexpectedElement(reader, element);
      }
      serverIdentitiesConfigurationBuilder.sslConfiguration().trustStore().path(path).relativeTo(relativeTo).password(keyStorePassword).provider(keyStoreProvider);
      trustStoreBuilder.name(name);
   }

   private void parseKerberos(ConfigurationReader reader, ServerIdentitiesConfigurationBuilder identitiesBuilder) {
      KerberosSecurityFactoryConfigurationBuilder builder = identitiesBuilder.addKerberosConfiguration();
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.KEYTAB_PATH, Attribute.PRINCIPAL);
      builder.keyTabPath(attributes[0]);
      builder.principal(attributes[1]);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case KEYTAB_PATH:
            case PRINCIPAL:
               // Already seen, ignore
               break;
            case RELATIVE_TO:
               builder.relativeTo(value);
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
      while (reader.inTag()) {
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
      builder.build(reader.getProperties());
   }

   private void parseDataSources(ConfigurationReader reader, ServerConfigurationBuilder builder) {
      DataSourcesConfigurationBuilder dataSources = builder.dataSources();
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case DATA_SOURCES: {
               parseDataSource(reader, element, builder, dataSources);
               reader.require(ConfigurationReader.ElementType.END_ELEMENT, null, Element.DATA_SOURCES);
               break;
            }
            case DATA_SOURCE: {
               parseDataSource(reader, element, builder, dataSources);
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseDataSource(ConfigurationReader reader, Element wrapper, ServerConfigurationBuilder builder, DataSourcesConfigurationBuilder dataSourcesBuilder) {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.JNDI_NAME);
      String name = attributes[0];
      String jndiName = attributes[1];
      DataSourceConfigurationBuilder dataSourceBuilder = dataSourcesBuilder.dataSource(name, jndiName);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
      EnumSet<Element> required = EnumSet.of(Element.CONNECTION_FACTORY);
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CONNECTION_FACTORY:
               parseDataSourceConnectionFactory(reader, builder, dataSourceBuilder);
               required.remove(Element.CONNECTION_FACTORY);
               break;
            case CONNECTION_POOL:
               parseDataSourceConnectionPool(reader, dataSourceBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
      if (!required.isEmpty()) {
         throw ParseUtils.missingRequiredElement(reader, required);
      }
   }

   private void parseDataSourceConnectionFactory(ConfigurationReader reader, ServerConfigurationBuilder builder, DataSourceConfigurationBuilder dataSourceBuilder) {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.DRIVER);
      dataSourceBuilder.driver(attributes[0]);
      boolean credentialSet = false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case DRIVER:
               // already parsed
               break;
            case USERNAME:
               dataSourceBuilder.username(value);
               break;
            case PASSWORD:
               dataSourceBuilder.password(value.toCharArray());
               credentialSet = true;
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
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CREDENTIAL_REFERENCE:
               if (credentialSet) {
                  throw Server.log.cannotOverrideCredential(Element.CONNECTION_FACTORY.toString(), Attribute.PASSWORD.toString());
               }
               dataSourceBuilder.password(parseCredentialReference(reader, builder));
               credentialSet = true;
               break;
            case CONNECTION_PROPERTIES:
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  dataSourceBuilder.addProperty(reader.getAttributeName(i), reader.getAttributeValue(i));
               }
               ParseUtils.requireNoContent(reader);
               break;
            case CONNECTION_PROPERTY:
               String name = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
               String value;
               if (reader.getAttributeCount() == 1) {
                  value = reader.getElementText();
               } else {
                  value = ParseUtils.requireAttributes(reader, Attribute.VALUE)[0];
                  ParseUtils.requireNoContent(reader);
               }
               dataSourceBuilder.addProperty(name, value);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader, element);
         }
      }
      if (!credentialSet) {
         throw Server.log.missingCredential(Element.CONNECTION_FACTORY.toString(), Attribute.PASSWORD.toString());
      }
   }

   private void parseDataSourceConnectionPool(ConfigurationReader reader, DataSourceConfigurationBuilder dataSourceBuilder) {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.MAX_SIZE);
      dataSourceBuilder.maxSize(Integer.parseInt(attributes[0]));
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
            case VALIDATE_ON_ACQUISITION:
               dataSourceBuilder.validateOnAcquisition(Long.parseLong(value));
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

   private void parseEndpoints(ConfigurationReader reader, ConfigurationBuilderHolder holder, ServerConfigurationBuilder builder) {
      String defaultSocketBinding = null;
      String defaultSecurityRealm = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         String value = reader.getAttributeValue(i);
         switch (attribute) {
            case SOCKET_BINDING:
               builder.endpoints().socketBinding(defaultSocketBinding = value);
               break;
            case SECURITY_REALM:
               builder.endpoints().securityRealm(defaultSecurityRealm = value);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         if (element == Element.ENDPOINT || element == Element.ENDPOINTS) {
            parseEndpoint(reader, holder, builder, element, defaultSocketBinding, defaultSecurityRealm);
         } else {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
      if (builder.endpoints().endpoints().isEmpty()) {
         if (defaultSocketBinding == null) {
            throw ParseUtils.missingRequired(reader, Collections.singleton(Attribute.SOCKET_BINDING));
         }
         EndpointConfigurationBuilder endpoint = builder.endpoints().addEndpoint(defaultSocketBinding);
         if (defaultSecurityRealm != null) {
            endpoint.securityRealm(defaultSecurityRealm).implicitConnectorSecurity(true);
         }
         configureDefaultEndpoint(reader, defaultSocketBinding, endpoint);
      }
   }

   private void parseEndpoint(ConfigurationReader reader, ConfigurationBuilderHolder holder, ServerConfigurationBuilder builder, Element endpointElement, String defaultSocketBinding, String defaultSecurityRealm) {
      final String socketBinding;
      if (defaultSocketBinding == null) {
         socketBinding = ParseUtils.requireAttributes(reader, Attribute.SOCKET_BINDING)[0];
      } else {
         String binding = reader.getAttributeValue(Attribute.SOCKET_BINDING);
         socketBinding = binding != null ? binding : defaultSocketBinding;
      }
      EndpointConfigurationBuilder endpoint = builder.endpoints().addEndpoint(socketBinding);
      String realm = reader.getAttributeValue(Attribute.SECURITY_REALM);
      final String securityRealm = realm != null ? realm : defaultSecurityRealm;
      if (securityRealm != null) {
         endpoint.securityRealm(securityRealm).implicitConnectorSecurity(reader.getSchema().since(11, 0));
      }
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         String value = reader.getAttributeValue(i);
         switch (attribute) {
            case SOCKET_BINDING:
            case SECURITY_REALM:
               // Already seen
               break;
            case ADMIN:
               endpoint.admin(Boolean.parseBoolean(value));
               break;
            case METRICS_AUTH:
               endpoint.metricsAuth(Boolean.parseBoolean(value));
               break;
            default:
               parseCommonConnectorAttributes(reader, i, builder, endpoint.singlePort());
               break;
         }
      }
      while (reader.inTag(endpointElement)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case IP_FILTER: {
               parseConnectorIpFilter(reader, endpoint.singlePort().ipFilter());
               break;
            }
            case CONNECTORS:
               // Wrapping element for YAML/JSON
               parseConnectors(reader, holder);
               break;
            default:
               reader.handleAny(holder);
               break;
         }
      }
      configureDefaultEndpoint(reader, socketBinding, endpoint);
   }

   private void parseConnectors(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      while (reader.inTag(Element.CONNECTORS)) {
         reader.getMapItem(Attribute.NAME);
         reader.handleAny(holder);
         reader.endMapItem();
      }
   }

   private void configureDefaultEndpoint(ConfigurationReader reader, String socketBinding, EndpointConfigurationBuilder endpoint) {
      if (endpoint.connectors().isEmpty()) {
         endpoint.addConnector(HotRodServerConfigurationBuilder.class).implicitConnector(true).startTransport(false).socketBinding(socketBinding);
         endpoint.addConnector(RespServerConfigurationBuilder.class).implicitConnector(true).startTransport(false).socketBinding(socketBinding);
         RestServerConfigurationBuilder rest = endpoint.addConnector(RestServerConfigurationBuilder.class).implicitConnector(true).startTransport(false).socketBinding(socketBinding);
         configureEndpoint(reader.getProperties(), endpoint, rest);
      }
   }

   public static void configureEndpoint(Properties properties, EndpointConfigurationBuilder endpoint, RestServerConfigurationBuilder builder) {
      if (endpoint.admin()) {
         String serverHome = properties.getProperty(Server.INFINISPAN_SERVER_HOME_PATH);
         builder.staticResources(Paths.get(serverHome, Server.DEFAULT_SERVER_STATIC_DIR));
      }
      builder.authentication().metricsAuth(endpoint.metricsAuth());
   }

   public static void parseCommonConnectorAttributes(ConfigurationReader reader, int index, ServerConfigurationBuilder serverBuilder, ProtocolServerConfigurationBuilder<?, ?> builder) {
      if (ParseUtils.isNoNamespaceAttribute(reader, index)) {
         Attribute attribute = Attribute.forName(reader.getAttributeName(index));
         String value = reader.getAttributeValue(index);
         switch (attribute) {
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
               if (reader.getSchema().since(14, 0)) {
                  throw ParseUtils.attributeRemoved(reader, index);
               } else {
                  CONFIG.ignoredAttribute(attribute.toString(), "14.0", attribute.name(), reader.getLocation().getLineNumber());
               }
               break;
            }
            default:
               throw ParseUtils.unexpectedAttribute(reader, index);
         }
      }
   }

   public static void parseCommonConnectorElements(ConfigurationReader reader, ProtocolServerConfigurationBuilder<?, ?> builder) {
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case IP_FILTER: {
            parseConnectorIpFilter(reader, builder.ipFilter());
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private static void parseConnectorIpFilter(ConfigurationReader reader, IpFilterConfigurationBuilder builder) {
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ACCEPT: {
               builder.allowFrom(ParseUtils.requireSingleAttribute(reader, Attribute.FROM));
               ParseUtils.requireNoContent(reader);
               break;
            }
            case REJECT: {
               builder.rejectFrom(ParseUtils.requireSingleAttribute(reader, Attribute.FROM));
               ParseUtils.requireNoContent(reader);
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   @Override
   public void readAttribute(ConfigurationReader reader, String elementName, int attributeIndex, ConfigurationBuilderHolder holder) {
      if (org.infinispan.configuration.parsing.Element.forName(elementName) == org.infinispan.configuration.parsing.Element.TRANSPORT) {
         ServerConfigurationBuilder serverBuilder = holder.getGlobalConfigurationBuilder().addModule(ServerConfigurationBuilder.class);
         String attributeName = reader.getAttributeName(attributeIndex);
         switch (Attribute.forName(attributeName)) {
            case SECURITY_REALM:
               serverBuilder.security().transport().securityRealm(reader.getAttributeValue(attributeIndex));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, attributeName);
         }
      } else {
         throw ParseUtils.unexpectedElement(reader, elementName);
      }
   }
}
