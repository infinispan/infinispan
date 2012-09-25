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
package org.infinispan.loaders.hbase.configuration;

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
 * HBaseCacheStoreConfigurationParser52.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class HBaseCacheStoreConfigurationParser52 implements ConfigurationParser<ConfigurationBuilderHolder> {

   private static final Namespace NAMESPACES[] = { new Namespace(Namespace.INFINISPAN_NS_BASE_URI, "hbase", Element.HBASE_STORE.getLocalName(), 5, 2),
         new Namespace("", Element.HBASE_STORE.getLocalName(), 0, 0) };

   public HBaseCacheStoreConfigurationParser52() {
   }

   @Override
   public Namespace[] getSupportedNamespaces() {
      return NAMESPACES;
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
