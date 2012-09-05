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
package org.infinispan.loaders.bdbje.configuration;

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
 * BdbjeCacheStoreConfigurationParser52.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class BdbjeCacheStoreConfigurationParser52 implements ConfigurationParser<ConfigurationBuilderHolder> {

   private static final Namespace NAMESPACES[] = {
         new Namespace(Namespace.INFINISPAN_NS_BASE_URI, "bdbje", Element.BDBJE_STORE.getLocalName(), 5, 2),
         new Namespace("", Element.BDBJE_STORE.getLocalName(), 0, 0) };

   public BdbjeCacheStoreConfigurationParser52() {
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
      case BDBJE_STORE: {
         parseBdbjeStore(reader, builder.loaders(), holder.getClassLoader());
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseBdbjeStore(final XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder,
         ClassLoader classLoader) throws XMLStreamException {
      BdbjeCacheStoreConfigurationBuilder builder = new BdbjeCacheStoreConfigurationBuilder(loadersBuilder);
      parseBdbjeStoreAttributes(reader, builder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Parser52.parseCommonStoreChildren(reader, builder);
      }
      loadersBuilder.addStore(builder);
   }

   private void parseBdbjeStoreAttributes(XMLExtendedStreamReader reader, BdbjeCacheStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case CACHE_DB_NAME_PREFIX: {
            builder.cacheDbNamePrefix(value);
            break;
         }
         case CATALOG_DB_NAME: {
            builder.catalogDbName(value);
            break;
         }
         case ENVIRONMENT_PROPERTIES_FILE: {
            builder.environmentPropertiesFile(value);
            break;
         }
         case EXPIRY_DB_PREFIX: {
            builder.expiryDbPrefix(value);
            break;
         }
         case LOCATION: {
            builder.location(value);
            break;
         }
         case LOCK_ACQUISITION_TIMEOUT: {
            builder.lockAcquistionTimeout(Long.parseLong(value));
            break;
         }
         case MAX_TX_RETRIES: {
            builder.maxTxRetries(Integer.parseInt(value));
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
