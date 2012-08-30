/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.loaders.mongodb.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.jboss.staxmapper.XMLExtendedStreamReader;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import static org.infinispan.util.StringPropertyReplacer.replaceProperties;

/**
 * Parses the configuration from the XML. For valid elements and attributes refer to {@link Element} and {@link Attribute}
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
public class MongoDBCacheStoreConfigurationParser implements ConfigurationParser<ConfigurationBuilderHolder> {

   private static final Namespace NAMESPACES[] = {
         new Namespace(Namespace.INFINISPAN_NS_BASE_URI, "mongodb", Element.MONGODB_STORE.getName(), 5, 3),
         new Namespace("", Element.MONGODB_STORE.getName(), 0, 0)};

   @Override
   public void readElement(XMLExtendedStreamReader xmlExtendedStreamReader, ConfigurationBuilderHolder configurationBuilderHolder)
         throws XMLStreamException {
      ConfigurationBuilder builder = configurationBuilderHolder.getCurrentConfigurationBuilder();

      Element element = Element.forName(xmlExtendedStreamReader.getLocalName());
      switch (element) {
         case MONGODB_STORE: {
            parseMongoDBStore(
                  xmlExtendedStreamReader,
                  builder.loaders(),
                  configurationBuilderHolder.getClassLoader()
            );
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(xmlExtendedStreamReader);
         }
      }
   }

   private void parseMongoDBStore(XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder, ClassLoader classLoader)
         throws XMLStreamException {
      MongoDBCacheStoreConfigurationBuilder builder = new MongoDBCacheStoreConfigurationBuilder(loadersBuilder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CONNECTION: {
               this.parseConnection(reader, builder);
               break;
            }
            case AUTHENTICATION: {
               this.parseAuthentication(reader, builder);
               break;
            }
            case STORAGE: {
               this.parseStorage(reader, builder);
               break;
            }
         }
      }
      loadersBuilder.addStore(builder);
   }

   private void parseStorage(XMLExtendedStreamReader reader, MongoDBCacheStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case DATABASE: {
               builder.database(value);
               break;
            }
            case COLLECTION:
               builder.collection(value);
               break;
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseConnection(XMLExtendedStreamReader reader, MongoDBCacheStoreConfigurationBuilder builder)
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
               builder.port(Integer.valueOf(value));
               break;
            case TIMEOUT:
               builder.timeout(Integer.valueOf(value));
            case ACKNOWLEDGMENT:
               builder.acknowledgment(Integer.valueOf(value));
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseAuthentication(XMLExtendedStreamReader reader, MongoDBCacheStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case USERNAME: {
               builder.username(value);
               break;
            }
            case PASSWORD:
               builder.password(value);
               break;
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   @Override
   public Namespace[] getSupportedNamespaces() {
      return NAMESPACES;
   }
}
