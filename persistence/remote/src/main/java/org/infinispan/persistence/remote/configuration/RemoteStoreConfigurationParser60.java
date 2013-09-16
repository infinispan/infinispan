package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser60;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

/**
 *
 * JdbcCacheStoreConfigurationParser60.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:remote:6.0", root = "remoteStore"),
   @Namespace(root = "remoteStore"),
})
public class RemoteStoreConfigurationParser60 implements ConfigurationParser {

   public RemoteStoreConfigurationParser60() {
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
      parseRemoteStoreAttributes(reader, builder, classLoader);

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
         case SERVERS: {
            parseServers(reader, builder);
            break;
         }
         default: {
            Parser60.parseCommonStoreChildren(reader, builder);
            break;
        }
         }
      }
      persistenceBuilder.addStore(builder);
   }

   private void parseAsyncTransportExecutor(final XMLExtendedStreamReader reader,
         final ExecutorFactoryConfigurationBuilder builder, ClassLoader classLoader) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case FACTORY: {
            builder.factory(Util.<ExecutorFactory> getInstance(value, classLoader));
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
         case PROPERTIES: {
            builder.withExecutorProperties(Parser60.parseProperties(reader));
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseConnectionPool(XMLExtendedStreamReader reader, ConnectionPoolConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
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

   private void parseServers(XMLExtendedStreamReader reader, RemoteStoreConfigurationBuilder builder)
         throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case SERVER: {
            parseServer(reader, builder.addServer());
            break;
         }
         default:
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseServer(XMLExtendedStreamReader reader, RemoteServerConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case HOST:
            builder.host(value);
            break;
         case PORT:
            builder.port(Integer.parseInt(value));
            break;
         default:
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseRemoteStoreAttributes(XMLExtendedStreamReader reader, RemoteStoreConfigurationBuilder builder, ClassLoader classLoader)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
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
            builder.pingOnStartup(Boolean.parseBoolean(value));
            break;
         }
         case PROTOCOL_VERSION: {
            builder.protocolVersion(value);
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
            Parser60.parseCommonStoreAttributes(reader, builder, attrName, value, i);
            break;
         }
         }
      }
   }
}
