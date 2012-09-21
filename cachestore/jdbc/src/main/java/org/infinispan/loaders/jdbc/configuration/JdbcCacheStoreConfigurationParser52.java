/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.jdbc.configuration;

import static org.infinispan.util.StringPropertyReplacer.replaceProperties;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser52;
import org.jboss.staxmapper.XMLExtendedStreamReader;

/**
 *
 * JdbcCacheStoreConfigurationParser52.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class JdbcCacheStoreConfigurationParser52 implements ConfigurationParser<ConfigurationBuilderHolder> {

   private static final Namespace NAMESPACES[] = {
         new Namespace(Namespace.INFINISPAN_NS_BASE_URI, "jdbc", Element.STRING_KEYED_JDBC_STORE.getLocalName(), 5, 2),
         new Namespace("", Element.STRING_KEYED_JDBC_STORE.getLocalName(), 0, 0),
         new Namespace(Namespace.INFINISPAN_NS_BASE_URI, "jdbc", Element.BINARY_KEYED_JDBC_STORE.getLocalName(), 5, 2),
         new Namespace("", Element.BINARY_KEYED_JDBC_STORE.getLocalName(), 0, 0),
         new Namespace(Namespace.INFINISPAN_NS_BASE_URI, "jdbc", Element.MIXED_KEYED_JDBC_STORE.getLocalName(), 5, 2),
         new Namespace("", Element.MIXED_KEYED_JDBC_STORE.getLocalName(), 0, 0) };

   public JdbcCacheStoreConfigurationParser52() {
   }

   @Override
   public Namespace[] getSupportedNamespaces() {
      return NAMESPACES;
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case STRING_KEYED_JDBC_STORE: {
         parseStringKeyedJdbcStore(reader, builder.loaders());
         break;
      }
      case BINARY_KEYED_JDBC_STORE: {
         parseBinaryKeyedJdbcStore(reader, builder.loaders());
         break;
      }
      case MIXED_KEYED_JDBC_STORE: {
         parseMixedKeyedJdbcStore(reader, builder.loaders());
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseStringKeyedJdbcStore(final XMLExtendedStreamReader reader,
         LoadersConfigurationBuilder loadersBuilder) throws XMLStreamException {
      JdbcStringBasedCacheStoreConfigurationBuilder builder = new JdbcStringBasedCacheStoreConfigurationBuilder(
            loadersBuilder);
      parseCommonJdbcStoreAttributes(reader, builder);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case STRING_KEYED_TABLE: {
            parseTable(reader, builder.table());
            break;
         }
         default: {
            Parser52.parseCommonStoreChildren(reader, builder);
            break;
         }
         }
      }
      loadersBuilder.addStore(builder);
   }

   private void parseBinaryKeyedJdbcStore(XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder)
         throws XMLStreamException {
      JdbcBinaryCacheStoreConfigurationBuilder builder = new JdbcBinaryCacheStoreConfigurationBuilder(
            loadersBuilder);
      parseCommonJdbcStoreAttributes(reader, builder);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case BINARY_KEYED_TABLE: {
            parseTable(reader, builder.table());
            break;
         }
         default: {
            Parser52.parseCommonStoreChildren(reader, builder);
            break;
         }
         }
      }
      loadersBuilder.addStore(builder);
   }

   private void parseCommonJdbcStoreAttributes(XMLExtendedStreamReader reader,
         AbstractJdbcCacheStoreConfigurationBuilder<?, ?> builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case CONNECTION_FACTORY_CLASS: {
            builder.connectionFactoryClass(value);
            break;
         }
         case DATASOURCE: {
            builder.datasource(value);
            break;
         }
         case DRIVER_CLASS: {
            builder.driverClass(value);
            break;
         }
         case PASSWORD: {
            builder.password(value);
            break;
         }
         case USERNAME: {
            builder.username(value);
            break;
         }
         default: {
            Parser52.parseCommonStoreAttributes(reader, i, builder);
            break;
         }
         }
      }
   }

   private void parseMixedKeyedJdbcStore(XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder)
         throws XMLStreamException {
      JdbcMixedCacheStoreConfigurationBuilder builder = new JdbcMixedCacheStoreConfigurationBuilder(loadersBuilder);
      parseCommonJdbcStoreAttributes(reader, builder);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case STRING_KEYED_TABLE: {
            parseTable(reader, builder.stringTable());
            break;
         }
         case BINARY_KEYED_TABLE: {
            parseTable(reader, builder.binaryTable());
            break;
         }
         default: {
            Parser52.parseCommonStoreChildren(reader, builder);
            break;
         }
         }
      }
      loadersBuilder.addStore(builder);
   }

   private void parseTable(XMLExtendedStreamReader reader, TableManipulationConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
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

   private void parseTableElements(XMLExtendedStreamReader reader, TableManipulationConfigurationBuilder builder)
         throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
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
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private Column parseTableElementAttributes(XMLExtendedStreamReader reader) throws XMLStreamException {
      Column column = new Column();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
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

   class Column {
      String name;
      String type;
   }
}
