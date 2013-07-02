package org.infinispan.loaders.cassandra.configuration;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser52;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;

/**
 *
 * CassandraCacheStoreConfigurationParser52.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Namespace(uri = "urn:infinispan:config:cassandra:5.2", root = "cassandraStore")
public class CassandraCacheStoreConfigurationParser52 implements ConfigurationParser {

   public CassandraCacheStoreConfigurationParser52() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case CASSANDRA_STORE: {
         parseCassandraStore(reader, builder.loaders(), holder.getClassLoader());
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseCassandraStore(final XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder,
         ClassLoader classLoader) throws XMLStreamException {
      CassandraCacheStoreConfigurationBuilder builder = new CassandraCacheStoreConfigurationBuilder(loadersBuilder);
      parseCassandraStoreAttributes(reader, builder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case SERVERS: {
            parseServers(reader, builder);
            break;
         }
         default:
            Parser52.parseCommonStoreChildren(reader, builder);
         }
      }
      loadersBuilder.addStore(builder);
   }

   private void parseServers(XMLExtendedStreamReader reader, CassandraCacheStoreConfigurationChildBuilder builder)
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

   private void parseServer(XMLExtendedStreamReader reader, CassandraServerConfigurationBuilder builder)
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

   private void parseCassandraStoreAttributes(XMLExtendedStreamReader reader,
         CassandraCacheStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case AUTO_CREATE_KEYSPACE: {
            builder.autoCreateKeyspace(Boolean.parseBoolean(value));
            break;
         }
         case CONFIGURATION_PROPERTIES_FILE: {
            builder.configurationPropertiesFile(value);
            break;
         }
         case ENTRY_COLUMN_FAMILY: {
            builder.entryColumnFamily(value);
            break;
         }
         case EXPIRATION_COLUMN_FAMILY: {
            builder.expirationColumnFamily(value);
            break;
         }
         case FRAMED: {
            builder.framed(true);
            break;
         }
         case KEY_MAPPER: {
            builder.keyMapper(value);
            break;
         }
         case KEY_SPACE: {
            builder.keySpace(value);
            break;
         }
         case PASSWORD: {
            builder.password(value);
            break;
         }
         case READ_CONSISTENCY_LEVEL: {
            builder.readConsistencyLevel(ConsistencyLevel.valueOf(value));
            break;
         }
         case USERNAME: {
            builder.username(value);
            break;
         }
         case WRITE_CONSISTENCY_LEVEL: {
            builder.writeConsistencyLevel(ConsistencyLevel.valueOf(value));
            break;
         }
         default: {
            Parser52.parseCommonStoreAttributes(reader, i, builder);
            break;
         }
         }
      }
   }
}
