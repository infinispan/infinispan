package org.infinispan.persistence.remote.configuration;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * Remote cache store parser.
 *
 * @author Galder Zamarreño
 * @since 9.0
 */
@MetaInfServices
@Namespace(root = "remote-store")
@Namespace(uri = "urn:infinispan:config:store:remote:*", root = "remote-store")
public class RemoteStoreConfigurationParser implements ConfigurationParser {

   private static final Log log = LogFactory.getLog(RemoteStoreConfigurationParser.class, Log.class);

   public RemoteStoreConfigurationParser() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case REMOTE_STORE: {
            parseRemoteStore(reader, builder.persistence(), holder.getClassLoader());
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseRemoteStore(final XMLExtendedStreamReader reader, PersistenceConfigurationBuilder persistenceBuilder,
         ClassLoader classLoader) throws XMLStreamException {
      RemoteStoreConfigurationBuilder builder = new RemoteStoreConfigurationBuilder(persistenceBuilder);
      parseRemoteStoreAttributes(reader, builder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
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
            case SERVER: {
               parseServer(reader, builder.addServer());
               break;
            }
            case SECURITY: {
               parseSecurity(reader, builder.remoteSecurity());
               break;
            }
            default: {
               Parser.parseStoreElement(reader, builder);
               break;
            }
         }
      }
      persistenceBuilder.addStore(builder);
   }

   private void parseSecurity(XMLExtendedStreamReader reader, SecurityConfigurationBuilder security) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
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

   private void parseAuthentication(XMLExtendedStreamReader reader, AuthenticationConfigurationBuilder authentication) throws XMLStreamException {
      authentication.enable();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
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
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
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
               parseAuthenticationDigest(reader, authentication);
               hasMech = true;
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseAuthenticationPlain(XMLExtendedStreamReader reader, AuthenticationConfigurationBuilder authentication) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.USERNAME.getLocalName(), Attribute.PASSWORD.getLocalName());
      authentication.saslMechanism("PLAIN").username(attributes[0]).password(attributes[1]);
      ParseUtils.requireNoContent(reader);
   }

   private void parseAuthenticationDigest(XMLExtendedStreamReader reader, AuthenticationConfigurationBuilder authentication) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.USERNAME.getLocalName(), Attribute.PASSWORD.getLocalName(), Attribute.REALM.getLocalName());
      authentication.saslMechanism("DIGEST-MD5").username(attributes[0]).password(attributes[1]).realm(attributes[2]);
      ParseUtils.requireNoContent(reader);
   }

   private void parseAuthenticationExternal(XMLExtendedStreamReader reader, AuthenticationConfigurationBuilder authentication) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      authentication.saslMechanism("EXTERNAL");
      ParseUtils.requireNoContent(reader);
   }

   private void parseEncryption(XMLExtendedStreamReader reader, SslConfigurationBuilder ssl) throws XMLStreamException {
      ssl.enable();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
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
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
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

   private void parseKeystore(XMLExtendedStreamReader reader, SslConfigurationBuilder ssl) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, true,
            Attribute.FILENAME.getLocalName(),
            Attribute.PASSWORD.getLocalName());
      ssl.keyStoreFileName(attributes[0]);
      ssl.keyStorePassword(attributes[1].toCharArray());

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FILENAME:
            case PASSWORD:
               // already processed
               break;
            case CERTIFICATE_PASSWORD: {
               ssl.keyStoreCertificatePassword(value.toCharArray());
               break;
            }
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

   private void parseTruststore(XMLExtendedStreamReader reader, SslConfigurationBuilder ssl) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, true,
            Attribute.FILENAME.getLocalName(),
            Attribute.PASSWORD.getLocalName());
      ssl.trustStoreFileName(attributes[0]);
      ssl.trustStorePassword(attributes[1].toCharArray());

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FILENAME:
            case PASSWORD:
               // already processed
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

   private void parseAsyncTransportExecutor(final XMLExtendedStreamReader reader,
         final ExecutorFactoryConfigurationBuilder builder, ClassLoader classLoader) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
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

      builder.withExecutorProperties(Parser.parseProperties(reader));
   }

   private void parseConnectionPool(XMLExtendedStreamReader reader, ConnectionPoolConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case EXHAUSTED_ACTION: {
               builder.exhaustedAction(ExhaustedAction.valueOf(value));
               break;
            }
            case MAX_ACTIVE: {
               builder.maxActive(Integer.parseInt(value));
               break;
            }
            case MAX_IDLE: {
               builder.maxIdle(Integer.parseInt(value));
               break;
            }
            case MAX_TOTAL: {
               builder.maxTotal(Integer.parseInt(value));
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
            case TEST_WHILE_IDLE: {
               builder.testWhileIdle(Boolean.parseBoolean(value));
               break;
            }
            case TIME_BETWEEN_EVICTION_RUNS: {
               builder.timeBetweenEvictionRuns(Long.parseLong(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseServer(XMLExtendedStreamReader reader, RemoteServerConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case HOST:
               builder.host(value);
               break;
            case PORT:
               builder.port(Integer.parseInt(value));
               break;
            case OUTBOUND_SOCKET_BINDING:
               log.ignoreXmlAttribute(attribute);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseRemoteStoreAttributes(XMLExtendedStreamReader reader, RemoteStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         String attrName = reader.getAttributeLocalName(i);
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
            case HOTROD_WRAPPING: {
               builder.hotRodWrapping(Boolean.parseBoolean(value));
               break;
            }
            case KEY_SIZE_ESTIMATE: {
               builder.keySizeEstimate(Integer.parseInt(value));
               break;
            }
            case MARSHALLER: {
               builder.marshaller(value);
               break;
            }
            case PING_ON_STARTUP: {
               if (!reader.getSchema().since(9, 1)) {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               } else {
                  log.ignoreXmlAttribute(attribute);
               }
               break;
            }
            case PROTOCOL_VERSION: {
               builder.protocolVersion(ProtocolVersion.parseVersion(value));
               break;
            }
            case RAW_VALUES: {
               builder.rawValues(Boolean.parseBoolean(value));
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
            case TRANSPORT_FACTORY: {
               builder.transportFactory(value);
               break;
            }
            case VALUE_SIZE_ESTIMATE: {
               builder.valueSizeEstimate(Integer.parseInt(value));
               break;
            }
            default: {
               Parser.parseStoreAttribute(reader, i, builder);
               break;
            }
         }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   @Override
   public Class<? extends ConfigurationBuilderInfo> getConfigurationBuilderInfo() {
      return RemoteStoreConfigurationBuilder.class;

   }
}
