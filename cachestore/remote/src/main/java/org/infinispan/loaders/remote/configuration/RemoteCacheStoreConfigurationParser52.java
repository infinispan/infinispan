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
package org.infinispan.loaders.remote.configuration;

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
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.loaders.remote.wrapper.EntryWrapper;
import org.infinispan.util.Util;
import org.jboss.staxmapper.XMLExtendedStreamReader;

/**
 *
 * JdbcCacheStoreConfigurationParser52.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RemoteCacheStoreConfigurationParser52 implements ConfigurationParser<ConfigurationBuilderHolder> {

   private static final Namespace NAMESPACES[] = {
         new Namespace(Namespace.INFINISPAN_NS_BASE_URI, "remote", Element.REMOTE_STORE.getLocalName(), 5, 2),
         new Namespace("", Element.REMOTE_STORE.getLocalName(), 0, 0) };

   public RemoteCacheStoreConfigurationParser52() {
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
      case REMOTE_STORE: {
         parseRemoteStore(reader, builder.loaders(), holder.getClassLoader());
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseRemoteStore(final XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder,
         ClassLoader classLoader) throws XMLStreamException {
      RemoteCacheStoreConfigurationBuilder builder = new RemoteCacheStoreConfigurationBuilder(loadersBuilder);
      parseRemoteStoreAttributes(reader, builder, classLoader);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case ASYNC_TRANSPORT_EXECUTOR: {
            parseAsyncTransportExecutor(reader, builder.asyncExecutorFactory(), classLoader);
            break;
         }
         case CONNECTION_POOL: {
            parseConnectionPool(reader, builder.connectionPool());
            break;
         }
         case SERVERS: {
            parseServers(reader, builder);
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

   private void parseAsyncTransportExecutor(final XMLExtendedStreamReader reader,
         final ExecutorFactoryConfigurationBuilder builder, ClassLoader classLoader) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case FACTORY: {
            builder.factory(Util.<ExecutorFactory> getInstance(value, classLoader));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case PROPERTIES: {
            builder.withExecutorProperties(Parser52.parseProperties(reader));
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseConnectionPool(XMLExtendedStreamReader reader, ConnectionPoolConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case EXHAUSTED_ACTION: {
            builder.exhaustedAction(ExhaustedAction.valueOf(value));
            break;
         }
         case MAX_ACTIVE: {
            builder.maxActive(Integer.parseInt(value));
            break;
         }
         case MAX_IDLE: {
            builder.maxIdle(Integer.parseInt(value));
            break;
         }
         case MAX_TOTAL: {
            builder.maxTotal(Integer.parseInt(value));
            break;
         }
         case MIN_EVICTABLE_IDLE_TIME: {
            builder.minEvictableIdleTime(Long.parseLong(value));
            break;
         }
         case MIN_IDLE: {
            builder.minIdle(Integer.parseInt(value));
            break;
         }
         case TEST_WHILE_IDLE: {
            builder.testWhileIdle(Boolean.parseBoolean(value));
            break;
         }
         case TIME_BETWEEN_EVICTION_RUNS: {
            builder.timeBetweenEvictionRuns(Long.parseLong(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseServers(XMLExtendedStreamReader reader, RemoteCacheStoreConfigurationBuilder builder)
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

   private void parseServer(XMLExtendedStreamReader reader, RemoteServerConfigurationBuilder builder)
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

   private void parseRemoteStoreAttributes(XMLExtendedStreamReader reader, RemoteCacheStoreConfigurationBuilder builder, ClassLoader classLoader)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case BALANCING_STRATEGY: {
            builder.balancingStrategy(value);
            break;
         }
         case CONNECT_TIMEOUT: {
            builder.connectionTimeout(Long.parseLong(value));
            break;
         }
         case ENTRY_WRAPPER: {
            builder.entryWrapper(Util.<EntryWrapper<?,?>>getInstance(value, classLoader));
            break;
         }
         case FORCE_RETURN_VALUES: {
            builder.forceReturnValues(Boolean.parseBoolean(value));
            break;
         }
         case HOTROD_WRAPPING: {
            builder.hotRodWrapping(Boolean.parseBoolean(value));
            break;
         }
         case KEY_SIZE_ESTIMATE: {
            builder.keySizeEstimate(Integer.parseInt(value));
            break;
         }
         case MARSHALLER: {
            builder.marshaller(value);
            break;
         }
         case PING_ON_STARTUP: {
            builder.pingOnStartup(Boolean.parseBoolean(value));
            break;
         }
         case PROTOCOL_VERSION: {
            builder.protocolVersion(value);
            break;
         }
         case RAW_VALUES: {
            builder.rawValues(Boolean.parseBoolean(value));
            break;
         }
         case REMOTE_CACHE_NAME: {
            builder.remoteCacheName(value);
            break;
         }
         case SOCKET_TIMEOUT: {
            builder.socketTimeout(Long.parseLong(value));
            break;
         }
         case TCP_NO_DELAY: {
            builder.tcpNoDelay(Boolean.parseBoolean(value));
            break;
         }
         case TRANSPORT_FACTORY: {
            builder.transportFactory(value);
            break;
         }
         case VALUE_SIZE_ESTIMATE: {
            builder.valueSizeEstimate(Integer.parseInt(value));
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
