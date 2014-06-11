package org.infinispan.persistence.rest.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser70;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.persistence.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

/**
 * Rest store configuration parser for Infinispan 7.0.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:store:rest:7.0", root = "rest-store"),
   @Namespace(root = "rest-store")
})
public class RestStoreConfigurationParser70 implements ConfigurationParser {

   private static final Log log = LogFactory.getLog(RestStoreConfigurationParser70.class, Log.class);

   public RestStoreConfigurationParser70() {
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
               Parser70.parseStoreElement(reader, builder);
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
         String value = replaceProperties(reader.getAttributeValue(i));
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

   private void parseConnectionPool(XMLExtendedStreamReader reader, ConnectionPoolConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
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
            case SOCKET_TIMEOUT:{
               builder.socketTimeout(Integer.parseInt(value));
               break;
            }
            case TCP_NO_DELAY:{
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
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         String attributeName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attributeName);
         switch (attribute) {
            case APPEND_CACHE_NAME_TO_PATH: {
               builder.appendCacheNameToPath(Boolean.parseBoolean(value));
               break;
            }
            case PATH: {
               builder.path(value);
               break;
            }
            case KEY_TO_STRING_MAPPER: {
               builder.key2StringMapper(value);
               break;
            }
            default: {
               Parser70.parseStoreAttribute(reader, i, builder);
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
