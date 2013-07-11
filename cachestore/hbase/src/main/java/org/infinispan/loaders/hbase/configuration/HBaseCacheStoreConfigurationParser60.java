package org.infinispan.loaders.hbase.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser52;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

/**
 *
 * HBaseCacheStoreConfigurationParser60.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:hbase:6.0", root = "hbaseStore"),
   @Namespace(root = "hbaseStore"),
})
public class HBaseCacheStoreConfigurationParser60 implements ConfigurationParser {

   public HBaseCacheStoreConfigurationParser60() {
   }


   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case HBASE_STORE: {
         parseHBaseStore(reader, builder.loaders(), holder.getClassLoader());
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseHBaseStore(final XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder, ClassLoader classLoader) throws XMLStreamException {
      HBaseCacheStoreConfigurationBuilder builder = new HBaseCacheStoreConfigurationBuilder(loadersBuilder);
      parseHBaseStoreAttributes(reader, builder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Parser52.parseCommonStoreChildren(reader, builder);
      }
      loadersBuilder.addStore(builder);
   }

   private void parseHBaseStoreAttributes(XMLExtendedStreamReader reader, HBaseCacheStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case AUTO_CREATE_TABLE: {
            builder.autoCreateTable(Boolean.parseBoolean(value));
            break;
         }
         case ENTRY_COLUMN_FAMILY: {
            builder.entryColumnFamily(value);
            break;
         }
         case ENTRY_TABLE: {
            builder.entryTable(value);
            break;
         }
         case ENTRY_VALUE_FIELD: {
            builder.entryValueField(value);
            break;
         }
         case EXPIRATION_COLUMN_FAMILY: {
            builder.expirationColumnFamily(value);
            break;
         }
         case EXPIRATION_TABLE: {
            builder.expirationTable(value);
            break;
         }
         case EXPIRATION_VALUE_FIELD: {
            builder.expirationValueField(value);
            break;
         }
         case HBASE_ZOOKEEPER_QUORUM_HOST: {
            builder.hbaseZookeeperQuorumHost(value);
            break;
         }
         case HBASE_ZOOKEEPER_CLIENT_PORT: {
            builder.hbaseZookeeperClientPort(Integer.parseInt(value));
            break;
         }
         case KEY_MAPPER: {
            builder.keyMapper(value);
            break;
         }
         case SHARED_TABLE: {
            builder.sharedTable(Boolean.parseBoolean(value));
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
