package org.infinispan.persistence.rest.configuration;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser60;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;


/**
 * RestStoreConfigurationParser60.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:store:rest:6.0", root = "restStore"),
   @Namespace(root = "restStore"),
})
public class RestStoreConfigurationParser60 implements ConfigurationParser {

   public RestStoreConfigurationParser60() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case REST_STORE: {
         parseRestStore(reader, builder.persistence(), holder.getClassLoader());
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseRestStore(final XMLExtendedStreamReader reader, PersistenceConfigurationBuilder loadersBuilder,
         ClassLoader classLoader) throws XMLStreamException {
      RestStoreConfigurationBuilder builder = new RestStoreConfigurationBuilder(loadersBuilder);
      parseRestStoreAttributes(reader, builder, classLoader);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case CONNECTION_POOL: {
            parseConnectionPool(reader, builder.connectionPool());
            break;
         }
         default: {
            Parser60.parseCommonStoreChildren(reader, builder);
            break;
         }
         }
      }
      loadersBuilder.addStore(builder);
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

   private void parseRestStoreAttributes(XMLExtendedStreamReader reader, RestStoreConfigurationBuilder builder, ClassLoader classLoader)
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
         case HOST: {
            builder.host(value);
            break;
         }
         case PATH: {
            builder.path(value);
            break;
         }
         case PORT: {
            builder.port(Integer.parseInt(value));
            break;
         }
         case KEY_TO_STRING_MAPPER: {
            builder.key2StringMapper(value);
            break;
         }
         default: {
            Parser60.parseCommonStoreAttributes(reader, builder, attributeName, value, i);
            break;
         }
         }
      }
   }
}
