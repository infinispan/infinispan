package org.infinispan.persistence.remote.configuration;

import static org.infinispan.configuration.parsing.ParseUtils.ignoreAttribute;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationParser.NAMESPACE;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.CacheParser;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.persistence.remote.configuration.global.RemoteContainerConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.global.RemoteContainersConfigurationBuilder;
import org.kohsuke.MetaInfServices;

/**
 * Remote cache store parser.
 *
 * @author Galder Zamarreño
 * @since 9.0
 */
@MetaInfServices
@Namespace(root = "remote-store")
@Namespace(root = "remote-cache-containers")
@Namespace(uri = NAMESPACE + "*", root = "remote-store")
@Namespace(uri = NAMESPACE + "*", root = "remote-cache-containers")
public class RemoteStoreConfigurationParser implements ConfigurationParser {
   public static final String PREFIX = "store:remote";
   public static final String NAMESPACE = Parser.NAMESPACE + PREFIX + ":";

   public RemoteStoreConfigurationParser() {
   }

   @Override
   public void readElement(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case REMOTE_STORE: {
            parseRemoteStore(reader, builder.persistence(), holder.getClassLoader());
            break;
         }
         case REMOTE_CACHE_CONTAINERS: {
            parseRemoteContainers(reader, holder);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseRemoteContainers(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case REMOTE_CACHE_CONTAINER: {
               parseRemoteContainer(reader, holder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseRemoteContainer(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      RemoteContainersConfigurationBuilder containersBuilder = holder.getGlobalConfigurationBuilder().addModule(RemoteContainersConfigurationBuilder.class);
      String name = reader.getAttributeValue(Attribute.NAME);
      RemoteContainerConfigurationBuilder builder = containersBuilder.addRemoteContainer(name != null ? name : "");

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME: {
               // Set during creation of the builder.
               continue;
            }
            case URI:
               builder.uri(value);
               break;
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ParseUtils.parseAttributes(reader, builder);
      builder.properties(Parser.parseProperties(reader, Element.REMOTE_CACHE_CONTAINER));
   }

   private void parseRemoteStore(final ConfigurationReader reader, PersistenceConfigurationBuilder persistenceBuilder,
                                 ClassLoader classLoader) {
      RemoteStoreConfigurationBuilder builder = new RemoteStoreConfigurationBuilder(persistenceBuilder);
      parseRemoteStoreAttributes(reader, builder);

      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ASYNC_TRANSPORT_EXECUTOR: {
               parseAsyncTransportExecutor(reader, builder.asyncExecutorFactory(), classLoader);
               break;
            }
            case CONNECTION_POOL: {
               parseConnectionPool(reader, builder.connectionPool());
               break;
            }
            case REMOTE_SERVER: {
               if (reader.getAttributeCount() > 0) {
                  parseServer(reader, builder.addServer());
               } else {
                  while (reader.inTag(Element.REMOTE_SERVER)) {
                     parseServer(reader, builder.addServer());
                  }
               }
               break;
            }
            case SECURITY: {
               parseSecurity(reader, builder.remoteSecurity());
               break;
            }
            default: {
               CacheParser.parseStoreElement(reader, builder);
               break;
            }
         }
      }
      persistenceBuilder.addStore(builder);
   }

   private void parseSecurity(ConfigurationReader reader, SecurityConfigurationBuilder security) {
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case AUTHENTICATION: {
               parseAuthentication(reader, security.authentication());
               break;
            }
            case ENCRYPTION: {
               parseEncryption(reader, security.ssl());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseAuthentication(ConfigurationReader reader, AuthenticationConfigurationBuilder authentication) {
      authentication.enable();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case SERVER_NAME: {
               authentication.serverName(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      boolean hasMech = false;
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case AUTH_PLAIN: {
               if (hasMech)
                  throw ParseUtils.unexpectedElement(reader);
               parseAuthenticationPlain(reader, authentication);
               hasMech = true;
               break;
            }
            case AUTH_DIGEST: {
               if (hasMech)
                  throw ParseUtils.unexpectedElement(reader);
               parseAuthenticationDigest(reader, authentication);
               hasMech = true;
               break;
            }
            case AUTH_EXTERNAL: {
               if (hasMech)
                  throw ParseUtils.unexpectedElement(reader);
               parseAuthenticationExternal(reader, authentication);
               hasMech = true;
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseAuthenticationPlain(ConfigurationReader reader, AuthenticationConfigurationBuilder authentication) {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.USERNAME.getLocalName(), Attribute.PASSWORD.getLocalName());
      authentication.saslMechanism("PLAIN").username(attributes[0]).password(attributes[1]);
      ParseUtils.requireNoContent(reader);
   }

   private void parseAuthenticationDigest(ConfigurationReader reader, AuthenticationConfigurationBuilder authentication) {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.USERNAME.getLocalName(), Attribute.PASSWORD.getLocalName(), Attribute.REALM.getLocalName());
      authentication.saslMechanism("DIGEST-MD5").username(attributes[0]).password(attributes[1]).realm(attributes[2]);
      ParseUtils.requireNoContent(reader);
   }

   private void parseAuthenticationExternal(ConfigurationReader reader, AuthenticationConfigurationBuilder authentication) {
      ParseUtils.requireNoAttributes(reader);
      authentication.saslMechanism("EXTERNAL");
      ParseUtils.requireNoContent(reader);
   }

   private void parseEncryption(ConfigurationReader reader, SslConfigurationBuilder ssl) {
      ssl.enable();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case PROTOCOL: {
               ssl.protocol(value);
               break;
            }
            case SNI_HOSTNAME: {
               ssl.sniHostName(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case KEYSTORE: {
               parseKeystore(reader, ssl);
               break;
            }
            case TRUSTSTORE: {
               parseTruststore(reader, ssl);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseKeystore(ConfigurationReader reader, SslConfigurationBuilder ssl) {
      String[] attributes = ParseUtils.requireAttributes(reader, true,
            Attribute.FILENAME.getLocalName(),
            Attribute.PASSWORD.getLocalName());
      ssl.keyStoreFileName(attributes[0]);
      ssl.keyStorePassword(attributes[1].toCharArray());

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case FILENAME:
            case PASSWORD:
               // already processed
               break;
            case KEY_ALIAS: {
               ssl.keyAlias(value);
               break;
            }
            case TYPE: {
               ssl.keyStoreType(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseTruststore(ConfigurationReader reader, SslConfigurationBuilder ssl) {
      String[] attributes = ParseUtils.requireAttributes(reader, true,
            Attribute.FILENAME.getLocalName(),
            Attribute.TYPE.getLocalName());
      ssl.trustStoreFileName(attributes[0]);
      ssl.trustStorePassword(attributes[1].toCharArray());

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case FILENAME:
            case PASSWORD:
               ssl.trustStorePassword(value.toCharArray());
               break;
            case TYPE: {
               ssl.trustStoreType(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseAsyncTransportExecutor(final ConfigurationReader reader,
                                            final ExecutorFactoryConfigurationBuilder builder, ClassLoader classLoader) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case FACTORY: {
               builder.factory(Util.getInstance(value, classLoader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      builder.withExecutorProperties(Parser.parseProperties(reader, Element.ASYNC_TRANSPORT_EXECUTOR));
   }

   private void parseConnectionPool(ConfigurationReader reader, ConnectionPoolConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case EXHAUSTED_ACTION: {
               builder.exhaustedAction(ExhaustedAction.valueOf(value));
               break;
            }
            case MAX_ACTIVE: {
               builder.maxActive(Integer.parseInt(value));
               break;
            }
            case MAX_PENDING_REQUESTS: {
               builder.maxPendingRequests(Integer.parseInt(value));
               break;
            }
            case MIN_EVICTABLE_IDLE_TIME: {
               builder.minEvictableIdleTime(Long.parseLong(value));
               break;
            }
            case MIN_IDLE: {
               builder.minIdle(Integer.parseInt(value));
               break;
            }
            case MAX_WAIT: {
               builder.maxWait(Integer.parseInt(value));
               break;
            }
            case MAX_TOTAL:
            case TEST_WHILE_IDLE:
            case TIME_BETWEEN_EVICTION_RUNS: {
               if (reader.getSchema().since(10, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ParseUtils.ignoreAttribute(reader, i);
               }
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseServer(ConfigurationReader reader, RemoteServerConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case HOST:
               builder.host(value);
               break;
            case PORT:
               builder.port(Integer.parseInt(value));
               break;
            case OUTBOUND_SOCKET_BINDING:
               ignoreAttribute(reader, i);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseRemoteStoreAttributes(ConfigurationReader reader, RemoteStoreConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         String attrName = reader.getAttributeName(i);
         Attribute attribute = Attribute.forName(attrName);
         switch (attribute) {
            case BALANCING_STRATEGY: {
               builder.balancingStrategy(value);
               break;
            }
            case CONNECT_TIMEOUT: {
               builder.connectionTimeout(Long.parseLong(value));
               break;
            }
            case FORCE_RETURN_VALUES: {
               builder.forceReturnValues(Boolean.parseBoolean(value));
               break;
            }
            case KEY_SIZE_ESTIMATE, VALUE_SIZE_ESTIMATE, HOTROD_WRAPPING, RAW_VALUES: {
               if (reader.getSchema().since(16,0)) {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               } else {
                  ParseUtils.ignoreAttribute(reader, i);
               }
               break;
            }
            case MARSHALLER: {
               builder.marshaller(value);
               break;
            }
            case PING_ON_STARTUP: {
               if (!reader.getSchema().since(9, 1)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ParseUtils.ignoreAttribute(reader, i);
               }
               break;
            }
            case PROTOCOL_VERSION: {
               builder.protocolVersion(ProtocolVersion.parseVersion(value));
               break;
            }
            case REMOTE_CACHE_CONTAINER: {
               builder.remoteCacheContainer(value);
               break;
            }
            case REMOTE_CACHE_NAME: {
               builder.remoteCacheName(value);
               break;
            }
            case SOCKET_TIMEOUT: {
               builder.socketTimeout(Long.parseLong(value));
               break;
            }
            case TCP_NO_DELAY: {
               builder.tcpNoDelay(Boolean.parseBoolean(value));
               break;
            }
            case URI: {
               builder.uri(value);
               break;
            }
            default: {
               CacheParser.parseStoreAttribute(reader, i, builder);
               break;
            }
         }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
