/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.as;

import static org.infinispan.util.StringPropertyReplacer.replaceProperties;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.AsyncStoreConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LegacyStoreConfigurationBuilder;
import org.infinispan.configuration.cache.CacheStoreConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.jboss.staxmapper.XMLExtendedStreamReader;

/**
 * This class implements the parser for AS7/EAP/JDG schema files
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ParserAS7 implements ConfigurationParser<ConfigurationBuilderHolder> {

   public static final String URN_JBOSS_DOMAIN = "urn:jboss:domain";
   public static final String URN_JBOSS_DOMAIN_INFINISPAN = "urn:jboss:domain:infinispan";

   private static final Namespace NAMESPACES[] = { new Namespace(URN_JBOSS_DOMAIN, Element.ROOT.getLocalName(), 1, 4),
         new Namespace(URN_JBOSS_DOMAIN, Element.ROOT.getLocalName(), 1, 3) };

   public ParserAS7() {
   }

   @Override
   public Namespace[] getSupportedNamespaces() {
      return NAMESPACES;
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      holder.setParserContext(ParserAS7.class, new ParserContextAS7());
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case PROFILE: {
            parseProfile(reader, holder);
            break;
         }
         case INTERFACES: {
            parseInterfaces(reader, holder);
            break;
         }
         case SOCKET_BINDING_GROUP: {
            parseSocketBindingGroup(reader, holder);
            break;
         }
         default: {
            reader.discardRemainder();
         }
         }
      }
   }

   private void parseProfile(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case SUBSYSTEM: {
            String ns = reader.getNamespaceURI();
            if (ns.contains("jgroups"))
               parseJGroupsSubsystem(reader, holder);
            else if (ns.contains("threads"))
               parseThreadsSubsystem(reader, holder);
            else if (ns.contains("infinispan"))
               parseInfinispanSubsystem(reader, holder);
            else
               reader.discardRemainder();
            break;
         }
         default: {
            reader.discardRemainder();
         }
         }
      }
   }

   private void parseThreadsSubsystem(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case THREAD_FACTORY: {
            parseThreadFactory(reader, holder);
            break;
         }
         case UNBOUNDED_QUEUE_THREAD_POOL: {
            reader.discardRemainder();
            break;
         }
         case BOUNDED_QUEUE_THREAD_POOL: {
            reader.discardRemainder();
            break;
         }
         case BLOCKING_BOUNDED_QUEUE_THREAD_POOL: {
            reader.discardRemainder();
            break;
         }
         case QUEUELESS_THREAD_POOL: {
            reader.discardRemainder();
            break;
         }
         case SCHEDULED_THREAD_POOL: {
            reader.discardRemainder();
            break;
         }
         default: {
            ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseThreadFactory(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      //FIXME implement me
      reader.discardRemainder();
   }

   private void parseJGroupsSubsystem(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      //FIXME implement me
      reader.discardRemainder();
   }

   private void parseInfinispanSubsystem(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case CACHE_CONTAINER: {
            parseContainer(reader, holder);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseContainer(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
         case ALIASES:
         case JNDI_NAME:
         case NAME:
         case START:{
            // IGNORE
            break;
         }
         case DEFAULT_CACHE: {
            // TODO
            break;
         }
         case LISTENER_EXECUTOR: {
            // TODO
            break;
         }
         case EVICTION_EXECUTOR: {
            // TODO
            break;
         }
         case REPLICATION_QUEUE_EXECUTOR: {
            // TODO
            break;
         }
         case MODULE: {
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
         case TRANSPORT: {
            parseTransport(reader, holder);
            break;
         }
         case LOCAL_CACHE: {
            parseLocalCache(reader, holder);
            break;
         }
         case INVALIDATION_CACHE: {
            parseInvalidationCache(reader, holder);
            break;
         }
         case REPLICATED_CACHE: {
            parseReplicatedCache(reader, holder);
            break;
         }
         case DISTRIBUTED_CACHE: {
            parseDistributedCache(reader, holder);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   protected void parseCacheAttribute(XMLExtendedStreamReader reader, int index, Attribute attribute, String value, ConfigurationBuilder builder) throws XMLStreamException {
      switch (attribute) {
      case NAME:
      case START:
      case JNDI_NAME: {
         // IGNORE
         break;
      }
      case BATCHING: {
         builder.invocationBatching().enable();
         break;
      }
      case INDEXING: {
         builder.indexing().enable();
         break;
      }
      case MODULE: {
         break;
      }
      default: {
         throw ParseUtils.unexpectedAttribute(reader, index);
      }
      }
   }

   private void parseTransport(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case STACK: {
            break;
         }
         case CLUSTER: {
            break;
         }
         case EXECUTOR: {
            break;
         }
         case LOCK_TIMEOUT: {
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseClusteredCacheAttribute(XMLExtendedStreamReader reader, int index, Attribute attribute, String value, ConfigurationBuilder builder, CacheMode cacheMode)
         throws XMLStreamException {
      switch (attribute) {
      case ASYNC_MARSHALLING: {
         builder.clustering().async().asyncMarshalling(Boolean.parseBoolean(value));
         break;
      }
      case MODE: {
         Mode mode = Mode.valueOf(value);
         builder.clustering().cacheMode(mode.apply(cacheMode));
         break;
      }
      case QUEUE_SIZE: {
         builder.clustering().async().replQueueMaxElements(Integer.parseInt(value));
         break;
      }
      case QUEUE_FLUSH_INTERVAL: {
         builder.clustering().async().replQueueInterval(Long.parseLong(value));
         break;
      }
      case REMOTE_TIMEOUT: {
         builder.clustering().sync().replTimeout(Long.parseLong(value));
         break;
      }
      default: {
         this.parseCacheAttribute(reader, index, attribute, value, builder);
      }
      }
   }

   private void parseLocalCache(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String name = reader.getAttributeValue(null, Attribute.NAME.getLocalName());
      ConfigurationBuilder builder = holder.newConfigurationBuilder(name);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         this.parseCacheAttribute(reader, i, attribute, value, builder);
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         this.parseCacheElement(reader, element, holder);
      }
   }

   private void parseDistributedCache(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String name = reader.getAttributeValue(null, Attribute.NAME.getLocalName());
      ConfigurationBuilder builder = holder.newConfigurationBuilder(name);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case OWNERS: {
            builder.clustering().hash().numOwners(Integer.parseInt(value));
            break;
         }
         case VIRTUAL_NODES: {
            builder.clustering().hash().numVirtualNodes(Integer.parseInt(value));
            break;
         }
         case L1_LIFESPAN: {
            builder.clustering().l1().lifespan(Long.parseLong(value));
            break;
         }
         default: {
            this.parseClusteredCacheAttribute(reader, i, attribute, value, builder, CacheMode.DIST_ASYNC);
         }
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case STATE_TRANSFER: {
            this.parseStateTransfer(reader, builder);
            break;
         }
         default: {
            this.parseCacheElement(reader, element, holder);
         }
         }
      }
   }

   private void parseReplicatedCache(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String name = reader.getAttributeValue(null, Attribute.NAME.getLocalName());
      ConfigurationBuilder builder = holder.newConfigurationBuilder(name);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         this.parseClusteredCacheAttribute(reader, i, attribute, value, builder, CacheMode.REPL_ASYNC);
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case STATE_TRANSFER: {
            this.parseStateTransfer(reader, builder);
            break;
         }
         default: {
            this.parseCacheElement(reader, element, holder);
         }
         }
      }
   }

   private void parseInvalidationCache(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String name = reader.getAttributeValue(null, Attribute.NAME.getLocalName());
      ConfigurationBuilder builder = holder.newConfigurationBuilder(name);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         this.parseClusteredCacheAttribute(reader, i, attribute, value, builder, CacheMode.INVALIDATION_ASYNC);
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         default: {
            this.parseCacheElement(reader, element, holder);
         }
         }
      }
   }

   protected void parseCacheElement(XMLExtendedStreamReader reader, Element element, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      switch (element) {
      case LOCKING: {
         this.parseLocking(reader, builder);
         break;
      }
      case TRANSACTION: {
         this.parseTransaction(reader, builder);
         break;
      }
      case EVICTION: {
         this.parseEviction(reader, builder);
         break;
      }
      case EXPIRATION: {
         this.parseExpiration(reader, builder);
         break;
      }
      case STORE: {
         this.parseCustomStore(reader, builder.loaders().addStore());
         break;
      }
      case FILE_STORE: {
         this.parseFileStore(reader, builder.loaders().addFileCacheStore());
         break;
      }
      default: {
         reader.handleAny(holder);
      }
      }
   }

   private void parseStateTransfer(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case ENABLED: {
            builder.clustering().stateTransfer().fetchInMemoryState(Boolean.parseBoolean(value));
            break;
         }
         case TIMEOUT: {
            builder.clustering().stateTransfer().timeout(Long.parseLong(value));
            break;
         }
         case CHUNK_SIZE: {
            builder.clustering().stateTransfer().chunkSize(Integer.parseInt(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   protected void parseLocking(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      // ModelNode for the cache add operation

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case ISOLATION: {
            builder.locking().isolationLevel(IsolationLevel.valueOf(value));
            break;
         }
         case STRIPING: {
            builder.locking().useLockStriping(Boolean.parseBoolean(value));
            break;
         }
         case ACQUIRE_TIMEOUT: {
            builder.locking().lockAcquisitionTimeout(Long.parseLong(value));
            break;
         }
         case CONCURRENCY_LEVEL: {
            builder.locking().concurrencyLevel(Integer.parseInt(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   protected void parseTransaction(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case STOP_TIMEOUT: {
            builder.transaction().cacheStopTimeout(Long.parseLong(value));
            break;
         }
         case MODE: {
            TransactionMode txMode = TransactionMode.valueOf(value);
            builder.transaction().transactionMode(txMode.getMode());
            builder.transaction().useSynchronization(!txMode.isXAEnabled());
            builder.transaction().recovery().enabled(txMode.isRecoveryEnabled());
            break;
         }
         case LOCKING: {
            builder.transaction().lockingMode(LockingMode.valueOf(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   protected void parseEviction(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case STRATEGY: {
            builder.eviction().strategy(EvictionStrategy.valueOf(value));
            break;
         }
         case MAX_ENTRIES: {
            builder.eviction().maxEntries(Integer.parseInt(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);

   }

   private void parseExpiration(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case MAX_IDLE: {
            builder.expiration().maxIdle(Long.parseLong(value));
            break;
         }
         case LIFESPAN: {
            builder.expiration().lifespan(Long.parseLong(value));
            break;
         }
         case INTERVAL: {
            builder.expiration().wakeUpInterval(Long.parseLong(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   protected void parseCustomStore(XMLExtendedStreamReader reader, LegacyStoreConfigurationBuilder builder) throws XMLStreamException {
      EnumSet<Attribute> required = EnumSet.of(Attribute.CLASS);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         required.remove(attribute);
         switch (attribute) {
         case CLASS: {
            break;
         }
         default: {
            parseStoreAttribute(reader, i, builder);
         }
         }
      }

      if (!required.isEmpty()) {
         throw ParseUtils.missingRequired(reader, required);
      }
   }

   protected void parseFileStore(XMLExtendedStreamReader reader, FileCacheStoreConfigurationBuilder storeBuilder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case RELATIVE_TO: {
            // FIXME
            break;
         }
         case PATH: {
            storeBuilder.location(value);
            break;
         }
         default: {
            parseStoreAttribute(reader, i, storeBuilder);
         }
         }
      }
      this.parseStoreElements(reader, storeBuilder);
   }

   public static void parseStoreAttribute(XMLExtendedStreamReader reader, int index, CacheStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      String value = reader.getAttributeValue(index);
      Attribute attribute = Attribute.forName(reader.getAttributeLocalName(index));
      storeBuilder.purgeSynchronously(true);
      switch (attribute) {
      case SHARED: {
         storeBuilder.loaders().shared(Boolean.parseBoolean(value));
         break;
      }
      case PRELOAD: {
         storeBuilder.loaders().preload(Boolean.parseBoolean(value));
         break;
      }
      case PASSIVATION: {
         storeBuilder.loaders().passivation(Boolean.parseBoolean(value));
         break;
      }
      case FETCH_STATE: {
         storeBuilder.fetchPersistentState(Boolean.parseBoolean(value));
         break;
      }
      case PURGE: {
         storeBuilder.purgeOnStartup(Boolean.parseBoolean(value));
         break;
      }
      case SINGLETON: {
         storeBuilder.singletonStore().enabled(Boolean.parseBoolean(value));
         break;
      }
      default: {
         throw ParseUtils.unexpectedAttribute(reader, index);
      }
      }
   }

   private void parseStoreElements(XMLExtendedStreamReader reader, CacheStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         parseStoreElement(reader, storeBuilder);
      }
   }

   public static void parseStoreElement(XMLExtendedStreamReader reader, CacheStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case WRITE_BEHIND: {
         parseStoreWriteBehind(reader, storeBuilder.async().enable());
         break;
      }
      case PROPERTY: {
         parseStoreProperty(reader, storeBuilder);
         break;
      }
      default:
         throw ParseUtils.unexpectedElement(reader);
      }
   }

   public static void parseStoreWriteBehind(XMLExtendedStreamReader reader, AsyncStoreConfigurationBuilder storeBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case FLUSH_LOCK_TIMEOUT: {
            storeBuilder.flushLockTimeout(Long.parseLong(value));
            break;
         }
         case MODIFICATION_QUEUE_SIZE: {
            storeBuilder.modificationQueueSize(Integer.parseInt(value));
            break;
         }
         case SHUTDOWN_TIMEOUT: {
            storeBuilder.shutdownTimeout(Long.parseLong(value));
            break;
         }
         case THREAD_POOL_SIZE: {
            storeBuilder.threadPoolSize(Integer.parseInt(value));
            break;
         }
         default:
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   public static void parseStoreProperty(XMLExtendedStreamReader reader, CacheStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      String property = ParseUtils.requireSingleAttribute(reader, Attribute.NAME.getLocalName());
      String value = reader.getElementText();
      storeBuilder.addProperty(property, value);
   }

   private void parseInterfaces(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case INTERFACE: {
            parseInterface(reader, holder);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseInterface(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String name = ParseUtils.requireSingleAttribute(reader, Attribute.NAME.getLocalName());
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case INET_ADDRESS: {
            String value = replaceProperties(ParseUtils.requireSingleAttribute(reader, Attribute.VALUE.getLocalName()));
            ParserContextAS7 parserContext = holder.getParserContext(ParserAS7.class);
            try {
               parserContext.addInterface(new NetworkInterface(name, InetAddress.getByName(value)));
            } catch (UnknownHostException e) {
               throw ParseUtils.invalidAttributeValue(reader, 0);
            }
            break;
         }
         default: {
            reader.discardRemainder();
         }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseSocketBindingGroup(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParserContextAS7 parserContext = holder.getParserContext(ParserAS7.class);
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName(), Attribute.DEFAULT_INTERFACE.getLocalName());
      String portOffset = reader.getAttributeValue(null, Attribute.PORT_OFFSET.getLocalName());
      SocketBindingGroup socketBindingGroup = new SocketBindingGroup(attributes[0], attributes[1], portOffset == null ? 0 : Integer.parseInt(replaceProperties(portOffset)));
      parserContext.addSocketBindingGroup(socketBindingGroup);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case SOCKET_BINDING: {
            parseSocketBinding(reader, holder);
            break;
         }
         case OUTBOUND_SOCKET_BINDING: {
            parseOutboundSocketBinding(reader, holder);
            break;
         }
         default: {
            reader.discardRemainder();
         }
         }
      }
   }

   private void parseSocketBinding(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName(), Attribute.PORT.getLocalName());
      ParserContextAS7 parserContext = holder.getParserContext(ParserAS7.class);
      SocketBindingGroup socketBindingGroup = parserContext.getCurrentSocketBindingGroup();
      String interfaceName = reader.getAttributeValue(null, Attribute.INTERFACE.getLocalName());
      if (interfaceName == null) {
         interfaceName = socketBindingGroup.defaultInterface();
      }
      InetAddress address = parserContext.getInterface(interfaceName).address();
      socketBindingGroup.addSocketBinding(new SocketBinding(attributes[0], address, Integer.parseInt(replaceProperties(attributes[1]))));
      ParseUtils.requireNoContent(reader);
   }

   private void parseOutboundSocketBinding(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String name = ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName())[0];
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case REMOTE_DESTINATION: {
            String[] attributes = ParseUtils.requireAttributes(reader, Attribute.HOST.getLocalName(), Attribute.PORT.getLocalName());
            ParserContextAS7 parserContext = holder.getParserContext(ParserAS7.class);
            SocketBindingGroup socketBindingGroup = parserContext.getCurrentSocketBindingGroup();
            socketBindingGroup.addOutboundSocketBinding(new OutboundSocketBinding(name, attributes[0], Integer.parseInt(attributes[1])));
            ParseUtils.requireNoContent(reader);
            return;
         }
         default: {
            reader.discardRemainder();
         }
         }
      }
      throw ParseUtils.missingRequiredElement(reader, EnumSet.of(Element.REMOTE_DESTINATION));
   }
}
