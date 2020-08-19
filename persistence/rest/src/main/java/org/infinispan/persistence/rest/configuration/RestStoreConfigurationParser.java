package org.infinispan.persistence.rest.configuration;

import static org.infinispan.persistence.rest.configuration.RestStoreConfigurationParser.NAMESPACE;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * Rest store configuration parser
 *
 * @author Galder Zamarreño
 * @since 9.0
 */
@MetaInfServices
@Namespace(root = "rest-store")
@Namespace(uri = NAMESPACE + "*", root = "rest-store")
public class RestStoreConfigurationParser implements ConfigurationParser {

   static final String NAMESPACE = Parser.NAMESPACE + "store:rest:";

   private static final Log log = LogFactory.getLog(RestStoreConfigurationParser.class, Log.class);

   public RestStoreConfigurationParser() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case REST_STORE: {
            parseRestStore(reader, builder.persistence());
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseRestStore(final XMLExtendedStreamReader reader, PersistenceConfigurationBuilder loadersBuilder) throws XMLStreamException {
      RestStoreConfigurationBuilder builder = new RestStoreConfigurationBuilder(loadersBuilder);
      parseRestStoreAttributes(reader, builder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CONNECTION_POOL: {
               parseConnectionPool(reader, builder.connectionPool());
               break;
            }
            case SERVER: {
               parseServer(reader, builder);
               break;
            }
            default: {
               Parser.parseStoreElement(reader, builder);
               break;
            }
         }
      }
      loadersBuilder.addStore(builder);
   }

   private void parseServer(XMLExtendedStreamReader reader, RestStoreConfigurationBuilder builder)
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
               log.ignoreXmlAttribute(attribute, reader.getLocation().getLineNumber(), reader.getLocation().getColumnNumber());
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseConnectionPool(XMLExtendedStreamReader reader, ConnectionPoolConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CONNECTION_TIMEOUT: {
               builder.connectionTimeout(Integer.parseInt(value));
               break;
            }
            case MAX_CONNECTIONS_PER_HOST: {
               builder.maxConnectionsPerHost(Integer.parseInt(value));
               break;
            }
            case MAX_TOTAL_CONNECTIONS: {
               builder.maxTotalConnections(Integer.parseInt(value));
               break;
            }
            case BUFFER_SIZE: {
               builder.bufferSize(Integer.parseInt(value));
               break;
            }
            case SOCKET_TIMEOUT: {
               builder.socketTimeout(Integer.parseInt(value));
               break;
            }
            case TCP_NO_DELAY: {
               builder.tcpNoDelay(Boolean.parseBoolean(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseRestStoreAttributes(XMLExtendedStreamReader reader, RestStoreConfigurationBuilder builder)
         throws XMLStreamException {
      boolean restStoreV2 = reader.getSchema().since(10, 1);
      boolean appendCacheName = false;
      String path = null;
      String cacheNameValue = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         String attributeName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attributeName);
         switch (attribute) {
            case APPEND_CACHE_NAME_TO_PATH: {
               if (restStoreV2) throw ParseUtils.unexpectedAttribute(reader, i);
               appendCacheName = Boolean.parseBoolean(value);
               break;
            }
            case PATH: {
               if (restStoreV2) throw ParseUtils.unexpectedAttribute(reader, i);
               path = value;
               break;
            }
            case CACHE_NAME: {
               cacheNameValue = value;
               break;
            }
            case KEY_TO_STRING_MAPPER: {
               builder.key2StringMapper(value);
               break;
            }
            case RAW_VALUES: {
               builder.rawValues(Boolean.parseBoolean(value));
               break;
            }
            case MAX_CONTENT_LENGTH: {
               builder.maxContentLength(Integer.parseInt(value));
               break;
            }
            default: {
               Parser.parseStoreAttribute(reader, i, builder);
               break;
            }
         }
      }

      String cacheName = restStoreV2 ? cacheNameValue : getCacheNameFromLegacy(appendCacheName, path);
      builder.cacheName(cacheName);
   }

   private String getCacheNameFromLegacy(boolean appendCacheName, String legacyPath) {
      if (legacyPath == null || !legacyPath.contains("/") || appendCacheName) return null;
      return legacyPath.substring(legacyPath.lastIndexOf("/") + 1);
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   @Override
   public Class<? extends ConfigurationBuilderInfo> getConfigurationBuilderInfo() {
      return RestStoreConfigurationBuilder.class;
   }
}
