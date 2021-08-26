package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.JdbcStoreConfigurationParser.NAMESPACE;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationParser;
import org.infinispan.persistence.jdbc.common.configuration.Attribute;
import org.infinispan.persistence.jdbc.common.configuration.Element;
import org.kohsuke.MetaInfServices;

/**
 * JDBC cache store configuration parser.
 *
 * @author Galder Zamarreño
 * @since 9.0
 */
@MetaInfServices(ConfigurationParser.class)
@Namespace(root = "string-keyed-jdbc-store")
@Namespace(root = "binary-keyed-jdbc-store")
@Namespace(root = "mixed-keyed-jdbc-store")
@Namespace(uri = NAMESPACE + "*", root = "string-keyed-jdbc-store")
@Namespace(uri = NAMESPACE + "*", root = "binary-keyed-jdbc-store", until = "9.0")
@Namespace(uri = NAMESPACE + "*", root = "mixed-keyed-jdbc-store", until = "9.0")
public class JdbcStoreConfigurationParser extends AbstractJdbcStoreConfigurationParser {

   static final String NAMESPACE = Parser.NAMESPACE + "store:jdbc:";

   public JdbcStoreConfigurationParser() {
   }

   @Override
   public void readElement(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case STRING_KEYED_JDBC_STORE: {
            parseStringKeyedJdbcStore(reader, builder.persistence());
            break;
         }
         case BINARY_KEYED_JDBC_STORE:
         case MIXED_KEYED_JDBC_STORE: {
            throw new CacheConfigurationException("Binary and Mixed Keyed JDBC stores were removed in 9.0. " +
                  "Please use JdbcStringBasedStore instead");
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseStringKeyedJdbcStore(final ConfigurationReader reader,
                                          PersistenceConfigurationBuilder persistenceBuilder) {
      JdbcStringBasedStoreConfigurationBuilder builder = new JdbcStringBasedStoreConfigurationBuilder(
            persistenceBuilder);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         if (!handleCommonAttributes(builder, attribute, value)) {
            if (attribute == Attribute.KEY_TO_STRING_MAPPER) {
               builder.key2StringMapper(value);
            } else {
               Parser.parseStoreAttribute(reader, i, builder);
            }
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         if (!handleCommonElement(builder, element, reader)) {
            if (element == Element.STRING_KEYED_TABLE) {
               parseTable(reader, builder.table());
            } else {
               Parser.parseStoreElement(reader, builder);
            }
         }
      }
      persistenceBuilder.addStore(builder);
   }

   private void parseTable(ConfigurationReader reader, TableManipulationConfigurationBuilder<?, ?> builder)
         {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case BATCH_SIZE: {
               builder.batchSize(Integer.parseInt(value));
               break;
            }
            case CREATE_ON_START: {
               builder.createOnStart(Boolean.parseBoolean(value));
               break;
            }
            case DROP_ON_EXIT: {
               builder.dropOnExit(Boolean.parseBoolean(value));
               break;
            }
            case FETCH_SIZE: {
               builder.fetchSize(Integer.parseInt(value));
               break;
            }
            case PREFIX: {
               builder.tableNamePrefix(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      parseTableElements(reader, builder);
   }

   private void parseTableElements(ConfigurationReader reader, TableManipulationConfigurationBuilder<?, ?> builder)
         {
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ID_COLUMN: {
               Column column = parseTableElementAttributes(reader);
               builder.idColumnName(column.name);
               builder.idColumnType(column.type);
               break;
            }
            case DATA_COLUMN: {
               Column column = parseTableElementAttributes(reader);
               builder.dataColumnName(column.name);
               builder.dataColumnType(column.type);
               break;
            }
            case TIMESTAMP_COLUMN: {
               Column column = parseTableElementAttributes(reader);
               builder.timestampColumnName(column.name);
               builder.timestampColumnType(column.type);
               break;
            }
            case SEGMENT_COLUMN: {
               Column column = parseTableElementAttributes(reader);
               builder.segmentColumnName(column.name);
               builder.segmentColumnType(column.type);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private Column parseTableElementAttributes(ConfigurationReader reader) {
      Column column = new Column();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME: {
               column.name = value;
               break;
            }
            case TYPE: {
               column.type = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
      return column;
   }

   static class Column {
      String name;
      String type;
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
