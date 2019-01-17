package org.infinispan.configuration.parsing;

import static org.infinispan.factories.KnownComponentNames.ASYNC_NOTIFICATION_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.PERSISTENCE_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.STATE_TRANSFER_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.shortened;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.executors.CachedThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ScheduledThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.GlobUtils;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ContentTypeConfigurationBuilder;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.infinispan.configuration.cache.EncodingConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryConfigurationBuilder;
import org.infinispan.configuration.cache.PartitionHandlingConfigurationBuilder;
import org.infinispan.configuration.cache.SecurityConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalRoleConfigurationBuilder;
import org.infinispan.configuration.global.GlobalStateConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.configuration.global.ThreadPoolConfigurationBuilder;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.persistence.cluster.ClusterLoader;
import org.infinispan.persistence.file.SingleFileStore;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.BuiltinJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.security.impl.CommonNameRoleMapper;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.kohsuke.MetaInfServices;

/**
 * This class implements the parser for Infinispan/AS7/EAP/JDG schema files
 *
 * @author Tristan Tarrant
 * @author Galder Zamarreño
 * @since 9.0
 */
@MetaInfServices

@Namespace(root = "infinispan")
@Namespace(uri = "urn:infinispan:config:*", root = "infinispan")
public class Parser implements ConfigurationParser {

   static final Log log = LogFactory.getLog(Parser.class);

   private final Map<String, DefaultThreadFactory> threadFactories = new HashMap<>();
   private final Map<String, ThreadPoolConfigurationBuilder> threadPools = new HashMap<>();
   private final Map<String, String> threadPoolToThreadFactory = new HashMap<>();

   public Parser() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      // Preload some default JGroups stacks
      holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.TCP(reader.getProperties()));
      holder.addJGroupsStack(BuiltinJGroupsChannelConfigurator.UDP(reader.getProperties()));
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CACHE_CONTAINER: {
               parseContainer(reader, holder);
               break;
            }
            case JGROUPS: {
               parseJGroups(reader, holder);
               break;
            }
            case THREADS: {
               parseThreads(reader, holder);
               break;
            }
            default: {
               reader.handleAny(holder);
               break;
            }
         }
      }
   }

   private void parseSerialization(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case MARSHALLER_CLASS: {
               builder.serialization().marshaller(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            case VERSION: {
               builder.serialization().version(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      parseAdvancedExternalizers(reader, holder);
   }

   private void parseAdvancedExternalizers(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ADVANCED_EXTERNALIZER: {
               int attributes = reader.getAttributeCount();
               AdvancedExternalizer<?> advancedExternalizer = null;
               Integer id = null;
               ParseUtils.requireAttributes(reader, Attribute.CLASS.getLocalName());
               for (int i = 0; i < attributes; i++) {
                  String value = reader.getAttributeValue(i);
                  Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
                  switch (attribute) {
                     case CLASS: {
                        advancedExternalizer = Util.getInstance(value, holder.getClassLoader());
                        break;
                     }
                     case ID: {
                        id = Integer.valueOf(value);
                        break;
                     }
                     default: {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                     }
                  }
               }

               ParseUtils.requireNoContent(reader);

               if (id != null) {
                  builder.serialization().addAdvancedExternalizer(id, advancedExternalizer);
               } else {
                  builder.serialization().addAdvancedExternalizer(advancedExternalizer);
               }
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseThreads(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case THREAD_FACTORY: {
               parseThreadFactory(reader);
               break;
            }
            case CACHED_THREAD_POOL: {
               parseCachedThreadPool(reader, holder);
               break;
            }
            case SCHEDULED_THREAD_POOL: {
               parseScheduledThreadPool(reader, holder);
               break;
            }
            case BLOCKING_BOUNDED_QUEUE_THREAD_POOL: {
               parseBlockingBoundedQueueThreadPool(reader, holder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

      // Link up thread factories with the thread pools that have referenced them
      for (Map.Entry<String, ThreadPoolConfigurationBuilder> entry : threadPools.entrySet()) {
         String threadFactoryName = threadPoolToThreadFactory.get(entry.getKey());
         if (threadFactoryName != null) {
            ThreadFactory threadFactory = threadFactories.get(threadFactoryName);
            entry.getValue().threadFactory(threadFactory);
         }
      }
   }

   private void parseBlockingBoundedQueueThreadPool(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ThreadPoolConfigurationBuilder builder = new ThreadPoolConfigurationBuilder(holder.getGlobalConfigurationBuilder());

      String name = null;
      String threadFactoryName = null;
      int maxThreads = 0;
      int coreThreads = 0;
      int queueLength = 0;
      long keepAlive = 0;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case NAME: {
               name = value;
               break;
            }
            case THREAD_FACTORY: {
               threadFactoryName = value;
               break;
            }
            case CORE_THREADS: {
               coreThreads = Integer.valueOf(value);
               break;
            }
            case MAX_THREADS: {
               maxThreads = Integer.valueOf(value);
               break;
            }
            case QUEUE_LENGTH: {
               queueLength = Integer.valueOf(value);
               break;
            }
            case KEEP_ALIVE_TIME: {
               keepAlive = Long.valueOf(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ThreadPoolExecutorFactory factory = new BlockingThreadPoolExecutorFactory(
            maxThreads, coreThreads, queueLength, keepAlive);
      builder.threadPoolFactory(factory);

      // Keep track of the thread pool to thread factory name mapping,
      // and wait until all threads section has been processed to link the
      // actual thread factories with the thread pools.
      threadPoolToThreadFactory.put(name, threadFactoryName);
      threadPools.put(name, builder);

      ParseUtils.requireNoContent(reader);
   }

   private void parseScheduledThreadPool(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ThreadPoolConfigurationBuilder builder = new ThreadPoolConfigurationBuilder(holder.getGlobalConfigurationBuilder());
      String name = null;
      String threadFactoryName = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case NAME: {
               name = value;
               break;
            }
            case THREAD_FACTORY: {
               threadFactoryName = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ThreadPoolExecutorFactory factory = ScheduledThreadPoolExecutorFactory.create();
      builder.threadPoolFactory(factory);

      // Keep track of the thread pool to thread factory name mapping,
      // and wait until all threads section has been processed to link the
      // actual thread factories with the thread pools.
      threadPoolToThreadFactory.put(name, threadFactoryName);
      threadPools.put(name, builder);

      ParseUtils.requireNoContent(reader);
   }

   private void parseCachedThreadPool(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ThreadPoolConfigurationBuilder builder = new ThreadPoolConfigurationBuilder(holder.getGlobalConfigurationBuilder());
      String name = null;
      String threadFactoryName = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case NAME: {
               name = value;
               break;
            }
            case THREAD_FACTORY: {
               threadFactoryName = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ThreadPoolExecutorFactory factory = CachedThreadPoolExecutorFactory.create();
      builder.threadPoolFactory(factory);

      // Keep track of the thread pool to thread factory name mapping,
      // and wait until all threads section has been processed to link the
      // actual thread factories with the thread pools.
      threadPoolToThreadFactory.put(name, threadFactoryName);
      threadPools.put(name, builder);

      ParseUtils.requireNoContent(reader);
   }

   private void parseThreadFactory(XMLExtendedStreamReader reader) throws XMLStreamException {
      String name = null;
      ThreadGroup threadGroup = null;
      String threadNamePattern = null;
      int priority = 1; // minimum priority

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case NAME: {
               name = value;
               break;
            }
            case GROUP_NAME: {
               threadGroup = new ThreadGroup(value);
               break;
            }
            case THREAD_NAME_PATTERN: {
               threadNamePattern = value;
               break;
            }
            case PRIORITY: {
               priority = Integer.valueOf(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      DefaultThreadFactory threadFactory = new DefaultThreadFactory(name,
            threadGroup, priority, threadNamePattern, null, null);
      threadFactories.put(name, threadFactory);
      ParseUtils.requireNoContent(reader);
   }

   private void parseJGroups(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      Transport transport = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case TRANSPORT:
               transport = Util.getInstance(value, holder.getClassLoader());
               break;
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      if (transport == null) {
         // Set up default transport
         holder.getGlobalConfigurationBuilder().transport().defaultTransport();
      } else {
         holder.getGlobalConfigurationBuilder().transport().transport(transport);
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case STACK_FILE:
               parseStackFile(reader, holder);
               break;
            case STACK:
               if (!reader.getSchema().since(10, 0)) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               parseJGroupsStack(reader, holder);
               break;
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void addJGroupsStackFile(ConfigurationBuilderHolder holder, String name, String path, Properties properties) {
      try (InputStream xml = FileLookupFactory.newInstance().lookupFileStrict(path, holder.getClassLoader())) {
         holder.addJGroupsStack(new FileJGroupsChannelConfigurator(name, path, xml, properties));
      } catch (IOException e) {
         throw log.jgroupsConfigurationNotFound(path);
      }
   }

   private void parseJGroupsStack(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String stackName = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
      EmbeddedJGroupsChannelConfigurator stackConfigurator = new EmbeddedJGroupsChannelConfigurator(stackName);
      String extend = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               break;
            case EXTENDS:
               extend = value;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      List<ProtocolConfiguration> stack = stackConfigurator.getProtocolStack();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case REMOTE_SITES:
               parseJGroupsRelay(reader, holder, stackConfigurator);
               break;
            default:
               // It should be an actual JGroups protocol
               String protocolName = reader.getLocalName();
               Map<String, String> protocolAttributes = new HashMap<>();
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  protocolAttributes.put(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
               }
               ParseUtils.requireNoContent(reader);
               stack.add(new ProtocolConfiguration(protocolName, protocolAttributes));
               break;
         }
      }
      holder.addJGroupsStack(stackConfigurator, extend);
   }

   private void parseJGroupsRelay(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, EmbeddedJGroupsChannelConfigurator stackConfigurator) throws XMLStreamException {
      String defaultStack = ParseUtils.requireSingleAttribute(reader, Attribute.DEFAULT_STACK);
      if (holder.getJGroupsStack(defaultStack) == null) {
         throw log.missingJGroupsStack(defaultStack);
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case REMOTE_SITE:
               String remoteSite = ParseUtils.requireAttributes(reader, Attribute.NAME)[0];
               String stack = defaultStack;
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
                  switch (attribute) {
                     case NAME:
                        break;
                     case STACK:
                        stack = reader.getAttributeValue(i);
                        if (holder.getJGroupsStack(stack) == null) {
                           throw log.missingJGroupsStack(stack);
                        }
                        break;
                     default:
                        throw ParseUtils.unexpectedAttribute(reader, i);
                  }
               }
               ParseUtils.requireNoContent(reader);
               stackConfigurator.addRemoteSite(remoteSite, holder.getJGroupsStack(stack));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseStackFile(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String attributes[] = ParseUtils.requireAttributes(reader, Attribute.NAME, Attribute.PATH);
      ParseUtils.requireNoContent(reader);

      addJGroupsStackFile(holder, attributes[0], attributes[1], reader.getProperties());
   }

   private void parseContainer(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      holder.pushScope(ParserScope.CACHE_CONTAINER);
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      if (!reader.getSchema().since(9, 0)) {
         builder.defaultCacheName(BasicCacheContainer.DEFAULT_CACHE_NAME);
      }
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case NAME: {
               builder.globalJmxStatistics().cacheManagerName(value);
               break;
            }
            case ALIASES: {
               log.ignoreXmlAttribute(attribute);
               break;
            }
            case DEFAULT_CACHE: {
               builder.defaultCacheName(value);
               break;
            }
            case JNDI_NAME: {
               log.ignoreXmlAttribute(attribute);
               break;
            }
            case START: {
               log.ignoreXmlAttribute(attribute);
               break;
            }
            case ASYNC_EXECUTOR: {
               builder.asyncThreadPool().read(createThreadPoolConfiguration(value, ASYNC_OPERATIONS_EXECUTOR));
               break;
            }
            case LISTENER_EXECUTOR: {
               builder.listenerThreadPool().read(
                     createThreadPoolConfiguration(value, ASYNC_NOTIFICATION_EXECUTOR));
               break;
            }
            case EVICTION_EXECUTOR:
               log.evictionExecutorDeprecated();
               // fallthrough
            case EXPIRATION_EXECUTOR: {
               builder.expirationThreadPool().read(
                     createThreadPoolConfiguration(value, EXPIRATION_SCHEDULED_EXECUTOR));
               break;
            }
            case REPLICATION_QUEUE_EXECUTOR: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.unexpectedAttribute(reader, attribute.getLocalName());
               } else {
                  log.ignoredReplicationQueueAttribute(attribute.getLocalName(), reader.getLocation().getLineNumber());
               }
               break;
            }
            case PERSISTENCE_EXECUTOR: {
               builder.persistenceThreadPool().read(
                     createThreadPoolConfiguration(value, PERSISTENCE_EXECUTOR));
               break;
            }
            case STATE_TRANSFER_EXECUTOR: {
               builder.stateTransferThreadPool().read(
                     createThreadPoolConfiguration(value, STATE_TRANSFER_EXECUTOR));
               break;
            }
            case MODULE: {
               log.ignoreXmlAttribute(attribute);
               break;
            }
            case STATISTICS: {
               builder.globalJmxStatistics().enabled(Boolean.parseBoolean(value));
               break;
            }
            case SHUTDOWN_HOOK: {
               builder.shutdown().hookBehavior(ShutdownHookBehavior.valueOf(value));
               break;
            }
            case ZERO_CAPACITY_NODE: {
               builder.zeroCapacityNode(Boolean.parseBoolean(value));
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
               parseLocalCache(reader, holder, false);
               break;
            }
            case LOCAL_CACHE_CONFIGURATION: {
               parseLocalCache(reader, holder, true);
               break;
            }
            case INVALIDATION_CACHE: {
               parseInvalidationCache(reader, holder, false);
               break;
            }
            case INVALIDATION_CACHE_CONFIGURATION: {
               parseInvalidationCache(reader, holder, true);
               break;
            }
            case REPLICATED_CACHE: {
               parseReplicatedCache(reader, holder, false);
               break;
            }
            case REPLICATED_CACHE_CONFIGURATION: {
               parseReplicatedCache(reader, holder, true);
               break;
            }
            case DISTRIBUTED_CACHE: {
               parseDistributedCache(reader, holder, false);
               break;
            }
            case DISTRIBUTED_CACHE_CONFIGURATION: {
               parseDistributedCache(reader, holder, true);
               break;
            }
            case SCATTERED_CACHE: {
               if (reader.getSchema().since(9, 1)) {
                  parseScatteredCache(reader, holder, false);
               } else {
                  throw ParseUtils.unexpectedElement(reader);
               }
               break;
            }
            case SCATTERED_CACHE_CONFIGURATION: {
               if (reader.getSchema().since(9, 1)) {
                  parseScatteredCache(reader, holder, true);
               } else {
                  throw ParseUtils.unexpectedElement(reader);
               }
               break;
            }
            case SERIALIZATION: {
               parseSerialization(reader, holder);
               break;
            }
            case MODULES: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.unexpectedElement(reader);
               } else {
                  parseModules(reader, holder);
               }
               break;
            }
            case JMX: {
               parseJmx(reader, holder);
               break;
            }
            case SECURITY: {
               parseGlobalSecurity(reader, holder);
               break;
            }
            case GLOBAL_STATE: {
               if (reader.getSchema().since(8, 1)) {
                  parseGlobalState(reader, holder);
               } else {
                  throw ParseUtils.unexpectedElement(reader);
               }
               break;
            }
            default: {
               reader.handleAny(holder);
            }
         }
      }
      holder.popScope();
   }

   private void parseGlobalSecurity(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case AUTHORIZATION: {
               parseGlobalAuthorization(reader, holder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseGlobalAuthorization(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalAuthorizationConfigurationBuilder builder = holder.getGlobalConfigurationBuilder().security().authorization().enable();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case AUDIT_LOGGER: {
               builder.auditLogger(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      PrincipalRoleMapper roleMapper = null;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case IDENTITY_ROLE_MAPPER:
               if (roleMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               roleMapper = new IdentityRoleMapper();
               break;
            case COMMON_NAME_ROLE_MAPPER:
               if (roleMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               roleMapper = new CommonNameRoleMapper();
               break;
            case CLUSTER_ROLE_MAPPER:
               if (roleMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               roleMapper = new ClusterRoleMapper();
               break;
            case CUSTOM_ROLE_MAPPER:
               if (roleMapper != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               roleMapper = parseCustomMapper(reader, holder);
               break;
            case ROLE: {
               parseGlobalRole(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      if (roleMapper != null) {
         builder.principalRoleMapper(roleMapper);
      }
   }

   private PrincipalRoleMapper parseCustomMapper(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String mapperClass = ParseUtils.requireSingleAttribute(reader, Attribute.CLASS.getLocalName());
      ParseUtils.requireNoContent(reader);
      return Util.getInstance(mapperClass, holder.getClassLoader());
   }

   private void parseGlobalRole(XMLExtendedStreamReader reader, GlobalAuthorizationConfigurationBuilder builder) throws XMLStreamException {
      String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName(), Attribute.PERMISSIONS.getLocalName());
      GlobalRoleConfigurationBuilder role = builder.role(attributes[0]);
      for(String permission : attributes[1].split("\\s+")) {
         role.permission(permission);
      }
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
            case PERMISSIONS: {
               // Already handled
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseJmx(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case JMX_DOMAIN: {
               builder.globalJmxStatistics().jmxDomain(value);
               break;
            }
            case MBEAN_SERVER_LOOKUP: {
               builder.globalJmxStatistics().mBeanServerLookup(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            case ALLOW_DUPLICATE_DOMAINS: {
               builder.globalJmxStatistics().allowDuplicateDomains(Boolean.valueOf(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      Properties properties = parseProperties(reader);
      builder.globalJmxStatistics().withProperties(properties);
   }

   private void parseModules(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         reader.handleAny(holder);
      }
   }

   private void parseTransport(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalConfigurationBuilder globalBuilder = holder.getGlobalConfigurationBuilder();
      if (holder.getGlobalConfigurationBuilder().transport().getTransport() == null) {
         holder.getGlobalConfigurationBuilder().transport().defaultTransport();
      }
      TransportConfigurationBuilder transport = holder.getGlobalConfigurationBuilder().transport();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case STACK: {
               JGroupsChannelConfigurator jGroupsStack = holder.getJGroupsStack(value);
               if (jGroupsStack == null) {
                  throw log.missingJGroupsStack(value);
               }
               Properties p = new Properties();
               p.put(JGroupsTransport.CHANNEL_CONFIGURATOR, jGroupsStack);
               p.put("stack", value);
               transport.withProperties(p);
               break;
            }
            case CLUSTER: {
               transport.clusterName(value);
               break;
            }
            case EXECUTOR: {
               transport.transportThreadPool().read(
                     createThreadPoolConfiguration(value, ASYNC_TRANSPORT_EXECUTOR));
               break;
            }
            case TOTAL_ORDER_EXECUTOR: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.unexpectedAttribute(reader, attribute.getLocalName());
               } else {
                  log.ignoredAttribute("total order executor", "9.0", attribute.getLocalName(), reader.getLocation().getLineNumber());
               }
            }
            case REMOTE_COMMAND_EXECUTOR: {
               transport.remoteCommandThreadPool().read(
                     createThreadPoolConfiguration(value, REMOTE_COMMAND_EXECUTOR));
               break;
            }
            case LOCK_TIMEOUT: {
               transport.distributedSyncTimeout(Long.valueOf(value));
               break;
            }
            case NODE_NAME: {
               transport.nodeName(value);
               for (DefaultThreadFactory threadFactory : threadFactories.values())
                  threadFactory.setNode(value);

               break;
            }
            case LOCKING:
               break;
            case MACHINE_ID: {
               transport.machineId(value);
               break;
            }
            case RACK_ID: {
               transport.rackId(value);
               break;
            }
            case SITE: {
               transport.siteId(value);
               globalBuilder.site().localSite(value);
               break;
            }
            case INITIAL_CLUSTER_SIZE: {
               if (reader.getSchema().since(8, 2)) {
                  transport.initialClusterSize(Integer.valueOf(value));
               } else {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               }
               break;
            }
            case INITIAL_CLUSTER_TIMEOUT: {
               if (reader.getSchema().since(8, 2)) {
                  transport.initialClusterTimeout(Long.parseLong(value), TimeUnit.MILLISECONDS);
               } else {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               }
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseGlobalState(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      GlobalStateConfigurationBuilder builder = holder.getGlobalConfigurationBuilder().globalState().enable();
      ConfigurationStorage storage = null;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PERSISTENT_LOCATION: {
               builder.persistentLocation(parseGlobalStatePath(reader));
               break;
            }
            case SHARED_PERSISTENT_LOCATION: {
               builder.sharedPersistentLocation(parseGlobalStatePath(reader));
               break;
            }
            case TEMPORARY_LOCATION: {
               builder.temporaryLocation(parseGlobalStatePath(reader));
               break;
            }
            case IMMUTABLE_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               storage = ConfigurationStorage.IMMUTABLE;
               break;
            }
            case VOLATILE_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               storage = ConfigurationStorage.VOLATILE;
               break;
            }
            case OVERLAY_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               ParseUtils.requireNoAttributes(reader);
               ParseUtils.requireNoContent(reader);
               storage = ConfigurationStorage.OVERLAY;
               break;
            }
            case MANAGED_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               } else {
                  throw log.managerConfigurationStorageUnavailable();
               }
            }
            case CUSTOM_CONFIGURATION_STORAGE: {
               if (storage != null) {
                  throw ParseUtils.unexpectedElement(reader);
               }
               storage = ConfigurationStorage.CUSTOM;
               builder.configurationStorageSupplier(parseCustomConfigurationStorage(reader, holder));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      if (storage != null) {
         builder.configurationStorage(storage);
      }
   }

   private String parseGlobalStatePath(XMLExtendedStreamReader reader) throws XMLStreamException {
      String path = ParseUtils.requireAttributes(reader, Attribute.PATH.getLocalName())[0];
      String relativeTo = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case RELATIVE_TO: {
               relativeTo = (String)reader.getProperty(reader.getAttributeValue(i));
               break;
            }
            case PATH: {
               // Handled above
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
      return ParseUtils.resolvePath(path, relativeTo);
   }

   private Supplier<? extends LocalConfigurationStorage> parseCustomConfigurationStorage(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      String storageClass = ParseUtils.requireSingleAttribute(reader, Attribute.CLASS.getLocalName());
      ParseUtils.requireNoContent(reader);
      return Util.getInstanceSupplier(storageClass, holder.getClassLoader());
   }

   private ThreadPoolConfiguration createThreadPoolConfiguration(String threadPoolName, String componentName) {
      ThreadPoolConfigurationBuilder threadPool = threadPools.get(threadPoolName);
      if (threadPool == null)
         throw log.undefinedThreadPoolName(threadPoolName);

      ThreadPoolConfiguration threadPoolConfiguration = threadPool.create();
      DefaultThreadFactory threadFactory = threadPoolConfiguration.threadFactory();
      threadFactory.setComponent(shortened(componentName));
      return threadPoolConfiguration;
   }

   private void parseLocalCache(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, boolean template) throws XMLStreamException {
      holder.pushScope(template ? ParserScope.CACHE_TEMPLATE : ParserScope.CACHE);
      String name = reader.getAttributeValue(null, Attribute.NAME.getLocalName());
      if (!template && GlobUtils.isGlob(name))
         throw log.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(null, Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      builder.clustering().cacheMode(CacheMode.LOCAL);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         this.parseCacheAttribute(reader, i, attribute, value, builder);
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         this.parseCacheElement(reader, element, holder);
      }
      holder.popScope();
   }

   private void parseCacheAttribute(XMLExtendedStreamReader reader,
         int index, Attribute attribute, String value, ConfigurationBuilder builder) throws XMLStreamException {
      switch (attribute) {
         case NAME:
         case CONFIGURATION:
            // Handled by the caller
            break;
         case START:
         case JNDI_NAME:
         case MODULE: {
            log.ignoreXmlAttribute(attribute);
            break;
         }
         case SIMPLE_CACHE:
            builder.simpleCache(Boolean.valueOf(value));
            break;
         case STATISTICS: {
            builder.jmxStatistics().enabled(Boolean.valueOf(value));
            break;
         }
         case STATISTICS_AVAILABLE: {
            builder.jmxStatistics().available(Boolean.valueOf(value));
            break;
         }
         case SPIN_DURATION: {
            log.ignoreXmlAttribute(attribute);
            break;
         }
         case UNRELIABLE_RETURN_VALUES: {
            builder.unsafe().unreliableReturnValues(Boolean.valueOf(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, index);
         }
      }
   }

   private void parseSharedStateCacheElement(XMLExtendedStreamReader reader, Element element, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
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

   private void parseBackups(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      // If backups is present then remove any existing backups as they were added by the default config.
      builder.sites().backups().clear();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case BACKUP: {
               this.parseBackup(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parsePartitionHandling(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      PartitionHandlingConfigurationBuilder ph = builder.clustering().partitionHandling();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED: {
               log.partitionHandlingConfigurationEnabledDeprecated();
               ph.enabled(Boolean.valueOf(value));
               break;
            }
            case WHEN_SPLIT: {
               ph.whenSplit(PartitionHandling.valueOf(value.toUpperCase()));
               break;
            }
            case MERGE_POLICY: {
               MergePolicy mp = MergePolicy.fromString(value);
               EntryMergePolicy mergePolicy = mp == MergePolicy.CUSTOM ? Util.getInstance(value, holder.getClassLoader()) : mp;
               ph.mergePolicy(mergePolicy);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseBackup(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      BackupConfigurationBuilder backup = builder.sites().addBackup();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SITE: {
               backup.site(value);
               break;
            }
            case STRATEGY: {
               backup.strategy(BackupConfiguration.BackupStrategy.valueOf(value));
               break;
            }
            case BACKUP_FAILURE_POLICY: {
               backup.backupFailurePolicy(BackupFailurePolicy.valueOf(value));
               break;
            }
            case TIMEOUT: {
               backup.replicationTimeout(Long.valueOf(value));
               break;
            }
            case ENABLED: {
               backup.enabled(Boolean.valueOf(value));
               break;
            }
            case USE_TWO_PHASE_COMMIT: {
               backup.useTwoPhaseCommit(Boolean.parseBoolean(value));
               break;
            }
            case FAILURE_POLICY_CLASS: {
               backup.failurePolicyClass(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      if (backup.site() == null) {
         throw ParseUtils.missingRequired(reader, Collections.singleton(Attribute.SITE));
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case TAKE_OFFLINE: {
               this.parseTakeOffline(reader, backup);
               break;
            }
            case STATE_TRANSFER: {
               this.parseXSiteStateTransfer(reader, backup);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseTakeOffline(XMLExtendedStreamReader reader, BackupConfigurationBuilder backup) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case TAKE_BACKUP_OFFLINE_AFTER_FAILURES: {
               backup.takeOffline().afterFailures(Integer.valueOf(value));
               break;
            }
            case TAKE_BACKUP_OFFLINE_MIN_WAIT: {
               backup.takeOffline().minTimeToWait(Long.valueOf(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseXSiteStateTransfer(XMLExtendedStreamReader reader, BackupConfigurationBuilder backup) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CHUNK_SIZE:
               backup.stateTransfer().chunkSize(Integer.parseInt(value));
               break;
            case TIMEOUT:
               backup.stateTransfer().timeout(Long.parseLong(value));
               break;
            case MAX_RETRIES:
               backup.stateTransfer().maxRetries(Integer.parseInt(value));
               break;
            case WAIT_TIME:
               backup.stateTransfer().waitTime(Long.parseLong(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseBackupFor(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      builder.sites().backupFor().reset();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case REMOTE_CACHE: {
               builder.sites().backupFor().remoteCache(value);
               break;
            }
            case REMOTE_SITE: {
               builder.sites().backupFor().remoteSite(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseCacheSecurity(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      SecurityConfigurationBuilder securityBuilder = builder.security();
      ParseUtils.requireNoAttributes(reader);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case AUTHORIZATION: {
               parseCacheAuthorization(reader, securityBuilder.authorization().enable());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseCacheAuthorization(XMLExtendedStreamReader reader, AuthorizationConfigurationBuilder authzBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED: {
               authzBuilder.enabled(Boolean.parseBoolean(value));
               break;
            }
            case ROLES: {
               for(String role : value.split("\\s+")) {
                  authzBuilder.role(role);
               }
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   protected void parseCacheElement(XMLExtendedStreamReader reader, Element element, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      switch (element) {
         case LOCKING: {
            this.parseLocking(reader, builder);
            break;
         }
         case TRANSACTION: {
            this.parseTransaction(reader, builder, holder);
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
         case ENCODING: {
            this.parseDataType(reader, builder, holder);
            break;
         }
         case PERSISTENCE: {
            this.parsePersistence(reader, holder);
            break;
         }
         case INDEXING: {
            this.parseIndexing(reader, holder);
            break;
         }
         case CUSTOM_INTERCEPTORS: {
            this.parseCustomInterceptors(reader, holder);
            break;
         }
         case VERSIONING: {
            parseVersioning(reader, holder);
            break;
         }
         case COMPATIBILITY: {
            parseCompatibility(reader, holder);
            break;
         }
         case STORE_AS_BINARY: {
            parseStoreAsBinary(reader, holder);
            break;
         }
         case MODULES: {
            if (reader.getSchema().since(9, 0)) {
               throw ParseUtils.unexpectedElement(reader);
            } else {
               parseModules(reader, holder);
            }
            break;
         }
         case DATA_CONTAINER: {
            parseDataContainer(reader, holder);
            break;
         }
         case MEMORY: {
            parseMemory(reader, holder);
            break;
         }
         case BACKUPS: {
            this.parseBackups(reader, builder);
            break;
         }
         case BACKUP_FOR: {
            this.parseBackupFor(reader, builder);
            break;
         }
         case PARTITION_HANDLING: {
            this.parsePartitionHandling(reader, holder);
            break;
         }
         case SECURITY: {
            this.parseCacheSecurity(reader, builder);
            break;
         }
         default: {
            reader.handleAny(holder);
         }
      }
   }

   private void parseDataContainer(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS:
               log.dataContainerConfigurationDeprecated();
               builder.dataContainer().dataContainer(Util.getInstance(value, holder.getClassLoader()));
               break;
            case KEY_EQUIVALENCE:
               builder.dataContainer().keyEquivalence(Util.<Equivalence>getInstance(value, holder.getClassLoader()));
               break;
            case VALUE_EQUIVALENCE:
               builder.dataContainer().valueEquivalence(Util.<Equivalence>getInstance(value, holder.getClassLoader()));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      Properties properties = new Properties();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PROPERTY: {
               parseProperty(reader, properties);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

      builder.dataContainer().withProperties(properties);
   }

   private void parseMemory(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      ParseUtils.requireNoAttributes(reader);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case OFF_HEAP:
               memoryBuilder.storageType(StorageType.OFF_HEAP);
               parseOffHeapMemoryAttributes(reader, holder);
               break;
            case OBJECT:
               memoryBuilder.storageType(StorageType.OBJECT);
               parseObjectMemoryAttributes(reader, holder);
               break;
            case BINARY:
               memoryBuilder.storageType(StorageType.BINARY);
               parseBinaryMemoryAttributes(reader, holder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseOffHeapMemoryAttributes(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SIZE:
               memoryBuilder.size(Long.parseLong(value));
               break;
            case EVICTION:
               memoryBuilder.evictionType(EvictionType.valueOf(value));
               break;
            case ADDRESS_COUNT:
               memoryBuilder.addressCount(Integer.parseInt(value));
               break;
            case STRATEGY:
               memoryBuilder.evictionStrategy(EvictionStrategy.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseObjectMemoryAttributes(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SIZE:
               memoryBuilder.size(Long.parseLong(value));
               break;
            case STRATEGY:
               memoryBuilder.evictionStrategy(EvictionStrategy.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseBinaryMemoryAttributes(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SIZE:
               memoryBuilder.size(Long.parseLong(value));
               break;
            case EVICTION:
               memoryBuilder.evictionType(EvictionType.valueOf(value));
               break;
            case STRATEGY:
               memoryBuilder.evictionStrategy(EvictionStrategy.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseStoreAsBinary(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      log.elementDeprecatedUseOther(Element.STORE_AS_BINARY, Element.MEMORY);
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Boolean binaryKeys = null;
      Boolean binaryValues = null;
      builder.memory().storageType(StorageType.BINARY);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case STORE_KEYS_AS_BINARY:
               binaryKeys = Boolean.parseBoolean(value);
               break;
            case STORE_VALUES_AS_BINARY:
               binaryValues = Boolean.parseBoolean(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      if (binaryKeys != null && !binaryKeys && binaryValues != null && !binaryValues)
         builder.memory().storageType(StorageType.OBJECT); // explicitly disable

      ParseUtils.requireNoContent(reader);
   }

   private void parseCompatibility(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      builder.compatibility().enable();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case MARSHALLER_CLASS:
               builder.compatibility().marshaller(Util.getInstance(value, holder.getClassLoader()));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseVersioning(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case VERSIONING_SCHEME:
               log.ignoredAttribute("versioning", "9.0", attribute.getLocalName(), reader.getLocation().getLineNumber());
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseCustomInterceptors(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case INTERCEPTOR: {
               parseInterceptor(reader, holder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseInterceptor(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case AFTER:
               interceptorBuilder.after(Util.loadClass(value, holder.getClassLoader()));
               break;
            case BEFORE:
               interceptorBuilder.before(Util.loadClass(value, holder.getClassLoader()));
               break;
            case CLASS:
               interceptorBuilder.interceptorClass(Util.loadClass(value, holder.getClassLoader()));
               break;
            case INDEX:
               interceptorBuilder.index(Integer.parseInt(value));
               break;
            case POSITION:
               interceptorBuilder.position(InterceptorConfiguration.Position.valueOf(value.toUpperCase()));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      interceptorBuilder.withProperties(parseProperties(reader));
   }

   protected void parseLocking(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
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
            case WRITE_SKEW_CHECK: {
               log.ignoredAttribute("write skew attribute", "9.0", attribute.getLocalName(), reader.getLocation().getLineNumber());
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   protected void parseTransaction(XMLExtendedStreamReader reader, ConfigurationBuilder builder, ConfigurationBuilderHolder holder) throws XMLStreamException {
      if (!reader.getSchema().since(9, 0)) {
         CacheMode cacheMode = builder.clustering().cacheMode();
         if (!cacheMode.isSynchronous()) {
            log.unsupportedAsyncCacheMode(cacheMode, cacheMode.toSync());
            builder.clustering().cacheMode(cacheMode.toSync());
         }
      }
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
               builder.transaction().useSynchronization(!txMode.isXAEnabled() && txMode.getMode().isTransactional());
               builder.transaction().recovery().enabled(txMode.isRecoveryEnabled());
               builder.invocationBatching().enable(txMode.isBatchingEnabled());
               break;
            }
            case LOCKING: {
               builder.transaction().lockingMode(LockingMode.valueOf(value));
               break;
            }
            case TRANSACTION_MANAGER_LOOKUP_CLASS: {
               builder.transaction().transactionManagerLookup(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            case REAPER_WAKE_UP_INTERVAL: {
               builder.transaction().reaperWakeUpInterval(Long.parseLong(value));
               break;
            }
            case COMPLETED_TX_TIMEOUT: {
               builder.transaction().completedTxTimeout(Long.parseLong(value));
               break;
            }
            case TRANSACTION_PROTOCOL: {
               builder.transaction().transactionProtocol(TransactionProtocol.valueOf(value));
               break;
            }
            case AUTO_COMMIT: {
               builder.transaction().autoCommit(Boolean.parseBoolean(value));
               break;
            }
            case RECOVERY_INFO_CACHE_NAME: {
               builder.transaction().recovery().recoveryInfoCacheName(value);
               break;
            }
            case NOTIFICATIONS: {
               builder.transaction().notifications(Boolean.parseBoolean(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   protected void parseDataType(XMLExtendedStreamReader reader, ConfigurationBuilder builder, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      EncodingConfigurationBuilder encodingBuilder = builder.encoding();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case KEY_DATA_TYPE:
               ContentTypeConfigurationBuilder keyBuilder = encodingBuilder.key();
               parseContentType(reader, holder, keyBuilder);
               ParseUtils.requireNoContent(reader);
               break;
            case VALUE_DATA_TYPE:
               ContentTypeConfigurationBuilder valueBuilder = encodingBuilder.value();
               parseContentType(reader, holder, valueBuilder);
               ParseUtils.requireNoContent(reader);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseContentType(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, ContentTypeConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case MEDIA_TYPE:
               builder.mediaType(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
   }

   private void parseEviction(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      log.elementDeprecatedUseOther(Element.EVICTION, Element.MEMORY);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case STRATEGY:
            case THREAD_POLICY:
            case TYPE:
               log.ignoreXmlElement(attribute);
               break;
            case MAX_ENTRIES:
               log.evictionMaxEntriesDeprecated();
            case SIZE:
               builder.memory().size(Long.parseLong(value));
               break;
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

   private void parseInvalidationCache(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, boolean template) throws XMLStreamException {
      holder.pushScope(template ? ParserScope.CACHE_TEMPLATE : ParserScope.CACHE);
      String name = reader.getAttributeValue(null, Attribute.NAME.getLocalName());
      if (!template && GlobUtils.isGlob(name))
         throw log.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(null, Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      CacheMode baseCacheMode = configuration == null ? CacheMode.INVALIDATION_SYNC : builder.clustering().cacheMode();
      builder.clustering().cacheMode(baseCacheMode);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case KEY_PARTITIONER: {
               builder.clustering().hash().keyPartitioner(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            default: {
               this.parseClusteredCacheAttribute(reader, i, attribute, value, builder, baseCacheMode);
            }
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            default: {
               this.parseCacheElement(reader, element, holder);
            }
         }
      }
      holder.popScope();
   }

   private void parseSegmentedCacheAttribute(XMLExtendedStreamReader reader,
                                             int index, Attribute attribute, String value, ConfigurationBuilder builder, ClassLoader classLoader, CacheMode baseCacheMode)
      throws XMLStreamException {
      switch (attribute) {
         case SEGMENTS: {
            builder.clustering().hash().numSegments(Integer.parseInt(value));
            break;
         }
         case CONSISTENT_HASH_FACTORY: {
            builder.clustering().hash().consistentHashFactory(Util.getInstance(value, classLoader));
            break;
         }
         case KEY_PARTITIONER: {
            if (reader.getSchema().since(8, 2)) {
               builder.clustering().hash().keyPartitioner(Util.getInstance(value, classLoader));
            } else {
               throw ParseUtils.unexpectedAttribute(reader, index);
            }
            break;
         }
         default: {
            this.parseClusteredCacheAttribute(reader, index, attribute, value, builder, baseCacheMode);
         }
      }
   }

   private void parseClusteredCacheAttribute(XMLExtendedStreamReader reader,
         int index, Attribute attribute, String value, ConfigurationBuilder builder, CacheMode baseCacheMode)
         throws XMLStreamException {
      switch (attribute) {
         case ASYNC_MARSHALLING: {
            if (reader.getSchema().since(9, 0)) {
               throw ParseUtils.unexpectedAttribute(reader, attribute.getLocalName());
            } else {
               log.ignoredReplicationQueueAttribute(attribute.getLocalName(), reader.getLocation().getLineNumber());
            }
            break;
         }
         case MODE: {
            Mode mode = Mode.valueOf(value);
            builder.clustering().cacheMode(mode.apply(baseCacheMode));
            break;
         }
         case QUEUE_SIZE: {
            log.ignoredReplicationQueueAttribute(attribute.getLocalName(), reader.getLocation().getLineNumber());
            break;
         }
         case QUEUE_FLUSH_INTERVAL: {
            log.ignoredReplicationQueueAttribute(attribute.getLocalName(), reader.getLocation().getLineNumber());
            break;
         }
         case REMOTE_TIMEOUT: {
            builder.clustering().remoteTimeout(Long.parseLong(value));
            break;
         }
         default: {
            this.parseCacheAttribute(reader, index, attribute, value, builder);
         }
      }
   }

   private void parseReplicatedCache(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, boolean template) throws XMLStreamException {
      holder.pushScope(template ? ParserScope.CACHE_TEMPLATE : ParserScope.CACHE);
      String name = reader.getAttributeValue(null, Attribute.NAME.getLocalName());
      if (!template && GlobUtils.isGlob(name))
         throw log.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(null, Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      CacheMode baseCacheMode = configuration == null ? CacheMode.REPL_SYNC : builder.clustering().cacheMode();
      builder.clustering().cacheMode(baseCacheMode);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         parseSegmentedCacheAttribute(reader, i, attribute, value, builder, holder.getClassLoader(), baseCacheMode);
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            default: {
               this.parseSharedStateCacheElement(reader, element, holder);
            }
         }
      }
      holder.popScope();
   }

   private void parseStateTransfer(XMLExtendedStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case AWAIT_INITIAL_TRANSFER: {
               builder.clustering().stateTransfer().awaitInitialTransfer(Boolean.parseBoolean(value));
               break;
            }
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

   private void parseDistributedCache(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, boolean template) throws XMLStreamException {
      holder.pushScope(template ? ParserScope.CACHE_TEMPLATE : ParserScope.CACHE);
      String name = reader.getAttributeValue(null, Attribute.NAME.getLocalName());
      if (!template && GlobUtils.isGlob(name))
         throw log.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(null, Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      CacheMode baseCacheMode = configuration == null ? CacheMode.DIST_SYNC : builder.clustering().cacheMode();
      builder.clustering().cacheMode(baseCacheMode);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case OWNERS: {
               builder.clustering().hash().numOwners(Integer.parseInt(value));
               break;
            }
            case L1_LIFESPAN: {
               long lifespan = Long.parseLong(value);
               if (lifespan > 0)
                  builder.clustering().l1().enable().lifespan(lifespan);
               else
                  builder.clustering().l1().disable();
               break;
            }
            case INVALIDATION_CLEANUP_TASK_FREQUENCY: {
               builder.clustering().l1().cleanupTaskFrequency(Long.parseLong(value));
               break;
            }
            case CAPACITY_FACTOR: {
               builder.clustering().hash().capacityFactor(Float.parseFloat(value));
               break;
            }
            default: {
               this.parseSegmentedCacheAttribute(reader, i, attribute, value, builder, holder.getClassLoader(), baseCacheMode);
            }
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case GROUPS: {
               parseGroups(reader, holder);
               break;
            }
            default: {
               this.parseSharedStateCacheElement(reader, element, holder);
            }
         }
      }
      holder.popScope();
   }

   private void parseGroups(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      ParseUtils.requireSingleAttribute(reader, "enabled");
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  builder.clustering().hash().groups().enabled();
               } else {
                  builder.clustering().hash().groups().disabled();
               }
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case GROUPER:
               String value = ParseUtils.readStringAttributeElement(reader, "class");
               builder.clustering().hash().groups().addGrouper(Util.getInstance(value, holder.getClassLoader()));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }

   }

   private void parseScatteredCache(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, boolean template) throws XMLStreamException {
      String name = reader.getAttributeValue(null, Attribute.NAME.getLocalName());
      if (!template && GlobUtils.isGlob(name))
         throw log.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(null, Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      CacheMode baseCacheMode = configuration == null ? CacheMode.SCATTERED_SYNC : builder.clustering().cacheMode();
      builder.clustering().cacheMode(baseCacheMode);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case INVALIDATION_BATCH_SIZE: {
               builder.clustering().invalidationBatchSize(Integer.parseInt(value));
               break;
            }
            default: {
               this.parseSegmentedCacheAttribute(reader, i, attribute, value, builder, holder.getClassLoader(), baseCacheMode);
            }
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            default: {
               this.parseSharedStateCacheElement(reader, element, holder);
            }
         }
      }
   }

   private ConfigurationBuilder getConfigurationBuilder(ConfigurationBuilderHolder holder, String name, boolean template, String baseConfigurationName) {
      if (holder.getNamedConfigurationBuilders().containsKey(name)) {
         throw log.duplicateCacheName(name);
      }
      ConfigurationBuilder builder = holder.newConfigurationBuilder(name).template(template);
      if (baseConfigurationName != null) {
         ConfigurationBuilder baseConfigurationBuilder = holder.getNamedConfigurationBuilders().get(baseConfigurationName);
         if (baseConfigurationBuilder == null) {
            throw log.undeclaredConfiguration(baseConfigurationName, name);
         }
         Configuration baseConfiguration = baseConfigurationBuilder.build();
         if (!baseConfiguration.isTemplate()) {
            throw log.noConfiguration(baseConfigurationName);
         }
         builder.read(baseConfiguration);
      }

      return builder;
   }

   private void parsePersistence(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PASSIVATION:
               builder.persistence().passivation(Boolean.parseBoolean(value));
               break;
            case AVAILABILITY_INTERVAL:
               builder.persistence().availabilityInterval(Integer.parseInt(value));
               break;
            case CONNECTION_ATTEMPTS:
               builder.persistence().connectionAttempts(Integer.parseInt(value));
               break;
            case CONNECTION_INTERVAL:
               builder.persistence().connectionInterval(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      // clear in order to override any configuration defined in default cache
      builder.persistence().clearStores();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLUSTER_LOADER:
               parseClusterLoader(reader, holder);
               break;
            case FILE_STORE:
               parseFileStore(reader, holder);
               break;
            case STORE:
               parseCustomStore(reader, holder);
               break;
            case LOADER:
               log.ignoreXmlElement(element);
               break;
            default:
               reader.handleAny(holder);
         }
      }
   }

   private void parseClusterLoader(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      ClusterLoaderConfigurationBuilder cclb = builder.persistence().addClusterLoader();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         String attrName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attrName);
         switch (attribute) {
            case REMOTE_TIMEOUT:
               cclb.remoteCallTimeout(Long.parseLong(value));
               break;
            default:
               parseStoreAttribute(reader, i, cclb);
               break;
         }
      }
      parseStoreElements(reader, cclb);
   }

   protected void parseFileStore(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      SingleFileStoreConfigurationBuilder storeBuilder = holder.getCurrentConfigurationBuilder().persistence().addSingleFileStore();
      String path = null;
      String relativeTo = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case RELATIVE_TO: {
               relativeTo = (String) reader.getProperty(value);
               break;
            }
            case PATH: {
               path = value;
               break;
            }
            case MAX_ENTRIES: {
               storeBuilder.maxEntries(Integer.valueOf(value));
               break;
            }
            case FRAGMENTATION_FACTOR: {
               storeBuilder.fragmentationFactor(Float.parseFloat(value));
               break;
            }
            default: {
               parseStoreAttribute(reader, i, storeBuilder);
            }
         }
      }
      path = ParseUtils.resolvePath(path, relativeTo);
      if (path != null) {
         storeBuilder.location(path);
      }
      this.parseStoreElements(reader, storeBuilder);
   }

   /**
    * This method is public static so that it can be reused by custom cache store/loader configuration parsers
    */
   public static void parseStoreAttribute(XMLExtendedStreamReader reader, int index, AbstractStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      String value = reader.getAttributeValue(index);
      Attribute attribute = Attribute.forName(reader.getAttributeLocalName(index));
      switch (attribute) {
         case SHARED: {
            storeBuilder.shared(Boolean.parseBoolean(value));
            break;
         }
         case READ_ONLY: {
            storeBuilder.ignoreModifications(Boolean.valueOf(value));
            break;
         }
         case PRELOAD: {
            storeBuilder.preload(Boolean.parseBoolean(value));
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
            storeBuilder.singleton().enabled(Boolean.parseBoolean(value));
            break;
         }
         case TRANSACTIONAL: {
            storeBuilder.transactional(Boolean.parseBoolean(value));
            break;
         }
         case MAX_BATCH_SIZE: {
            storeBuilder.maxBatchSize(Integer.parseInt(value));
            break;
         }
         case SEGMENTED: {
            storeBuilder.segmented(Boolean.parseBoolean(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, index);
         }
      }
   }

   private void parseStoreElements(XMLExtendedStreamReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         parseStoreElement(reader, storeBuilder);
      }
   }

   public static void parseStoreElement(XMLExtendedStreamReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
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
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   public static void parseStoreWriteBehind(XMLExtendedStreamReader reader, AsyncStoreConfigurationBuilder<?> storeBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FLUSH_LOCK_TIMEOUT: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.unexpectedAttribute(reader, attribute.getLocalName());
               } else {
                  storeBuilder.flushLockTimeout(Long.parseLong(value));
               }
               break;
            }
            case MODIFICATION_QUEUE_SIZE: {
               storeBuilder.modificationQueueSize(Integer.parseInt(value));
               break;
            }
            case FAIL_SILENTLY:
               storeBuilder.failSilently(Boolean.parseBoolean(value));
               break;
            case SHUTDOWN_TIMEOUT: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.unexpectedAttribute(reader, attribute.getLocalName());
               } else {
                  storeBuilder.shutdownTimeout(Long.parseLong(value));
               }
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

   public static void parseStoreProperty(XMLExtendedStreamReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      String property = ParseUtils.requireSingleAttribute(reader, Attribute.NAME.getLocalName());
      String value = reader.getElementText();
      storeBuilder.addProperty(property, value);
   }

   private void parseCustomStore(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Boolean fetchPersistentState = null;
      Boolean ignoreModifications = null;
      Boolean purgeOnStartup = null;
      Boolean preload = null;
      Boolean shared = null;
      Boolean singleton = null;
      Boolean transactional = null;
      CacheLoader store = null;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS:
               store = Util.getInstance(value, holder.getClassLoader());
               break;
            case FETCH_STATE:
               fetchPersistentState = Boolean.valueOf(value);
               break;
            case READ_ONLY:
               ignoreModifications = Boolean.valueOf(value);
               break;
            case PURGE:
               purgeOnStartup = Boolean.valueOf(value);
               break;
            case PRELOAD:
               preload = Boolean.parseBoolean(value);
               break;
            case SHARED:
               shared = Boolean.parseBoolean(value);
               break;
            case SINGLETON:
               singleton = Boolean.parseBoolean(value);
               break;
            case TRANSACTIONAL:
               transactional = Boolean.parseBoolean(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      if (store != null) {
         if (store instanceof SingleFileStore) {
            SingleFileStoreConfigurationBuilder sfs = builder.persistence().addSingleFileStore();
            if (fetchPersistentState != null)
               sfs.fetchPersistentState(fetchPersistentState);
            if (ignoreModifications != null)
               sfs.ignoreModifications(ignoreModifications);
            if (purgeOnStartup != null)
               sfs.purgeOnStartup(purgeOnStartup);
            if (preload != null)
               sfs.preload(preload);
            if (shared != null)
               sfs.shared(shared);
            if (singleton != null)
               sfs.singleton().enabled(singleton);
            if (transactional != null)
               sfs.transactional(transactional);
            parseStoreElements(reader, sfs);
         } else if (store instanceof ClusterLoader) {
            ClusterLoaderConfigurationBuilder cscb = builder.persistence().addClusterLoader();
            parseStoreElements(reader, cscb);
         } else {
            ConfiguredBy annotation = store.getClass().getAnnotation(ConfiguredBy.class);
            Class<? extends StoreConfigurationBuilder> builderClass = null;
            if (annotation != null) {
               Class<?> configuredBy = annotation.value();
               if (configuredBy != null) {
                  BuiltBy builtBy = configuredBy.getAnnotation(BuiltBy.class);
                  builderClass = builtBy.value().asSubclass(StoreConfigurationBuilder.class);
               }
            }

            StoreConfigurationBuilder configBuilder;
            // If they don't specify a builder just use the custom configuration builder and set the class
            if (builderClass == null) {
               configBuilder = builder.persistence().addStore(CustomStoreConfigurationBuilder.class).customStoreClass(
                     store.getClass());
            } else {
               configBuilder = builder.persistence().addStore(builderClass);
            }

            if (fetchPersistentState != null)
               configBuilder.fetchPersistentState(fetchPersistentState);
            if (ignoreModifications != null)
               configBuilder.ignoreModifications(ignoreModifications);
            if (purgeOnStartup != null)
               configBuilder.purgeOnStartup(purgeOnStartup);
            if (preload != null)
               configBuilder.preload(preload);
            if (shared != null)
               configBuilder.shared(shared);
            if (transactional != null)
               configBuilder.transactional(transactional);

            parseStoreElements(reader, configBuilder);
         }
      }
   }

   private void parseIndexing(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case INDEX:
               Index index = Index.valueOf(value);
               builder.indexing().index(index);
               break;
            case AUTO_CONFIG:
               builder.indexing().autoConfig(Boolean.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      Properties indexingProperties = new Properties();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case INDEXED_ENTITIES: {
               parseIndexedEntities(reader, holder, builder);
               break;
            }
            case PROPERTY: {
               parseProperty(reader, indexingProperties);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      builder.indexing().withProperties(indexingProperties);
   }

   private void parseIndexedEntities(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder, ConfigurationBuilder builder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case INDEXED_ENTITY: {
               ParseUtils.requireNoAttributes(reader);
               String className = reader.getElementText();
               Class<?> indexedEntity = Util.loadClass(className, holder.getClassLoader());
               builder.indexing().addIndexedEntity(indexedEntity);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private static void parseProperty(XMLExtendedStreamReader reader, Properties properties) throws XMLStreamException {
      int attributes = reader.getAttributeCount();
      ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName());
      String key = null;
      String propertyValue;
      for (int i = 0; i < attributes; i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME: {
               key = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      propertyValue = reader.getElementText();
      properties.setProperty(key, propertyValue);
   }

   public static Properties parseProperties(final XMLExtendedStreamReader reader) throws XMLStreamException {
      Properties properties = new Properties();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PROPERTY: {
               parseProperty(reader, properties);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      return properties;
   }

   public enum TransactionMode {
      NONE(org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL, false, false, false),
      BATCH(org.infinispan.transaction.TransactionMode.TRANSACTIONAL, false, false, true),
      NON_XA(org.infinispan.transaction.TransactionMode.TRANSACTIONAL, false, false, false),
      NON_DURABLE_XA(org.infinispan.transaction.TransactionMode.TRANSACTIONAL, true, false, false),
      FULL_XA(org.infinispan.transaction.TransactionMode.TRANSACTIONAL, true, true, false),
      ;
      private final org.infinispan.transaction.TransactionMode mode;
      private final boolean xaEnabled;
      private final boolean recoveryEnabled;
      private final boolean batchingEnabled;

      TransactionMode(org.infinispan.transaction.TransactionMode mode, boolean xaEnabled, boolean recoveryEnabled, boolean batchingEnabled) {
         this.mode = mode;
         this.xaEnabled = xaEnabled;
         this.recoveryEnabled = recoveryEnabled;
         this.batchingEnabled = batchingEnabled;
      }

      public static TransactionMode fromConfiguration(org.infinispan.transaction.TransactionMode mode, boolean xaEnabled, boolean recoveryEnabled, boolean batchingEnabled) {
         if (mode==org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL) {
            return NONE;
         }
         for(TransactionMode txMode : TransactionMode.values()) {
            if (txMode.mode == mode && txMode.xaEnabled == xaEnabled && txMode.recoveryEnabled == recoveryEnabled && txMode.batchingEnabled == batchingEnabled)
               return txMode;
         }
         throw log.unknownTransactionConfiguration(mode, xaEnabled, recoveryEnabled, batchingEnabled);
      }

      public org.infinispan.transaction.TransactionMode getMode() {
         return this.mode;
      }

      public boolean isXAEnabled() {
         return this.xaEnabled;
      }

      public boolean isRecoveryEnabled() {
         return this.recoveryEnabled;
      }

      public boolean isBatchingEnabled() {
         return batchingEnabled;
      }
   }

   public enum Mode {
      SYNC(true),
      ASYNC(false),
      ;
      private final boolean sync;
      Mode(boolean sync) {
         this.sync = sync;
      }

      public static Mode forCacheMode(CacheMode mode) {
         return mode.isSynchronous() ? SYNC : ASYNC;
      }

      public CacheMode apply(CacheMode mode) {
         return this.sync ? mode.toSync() : mode.toAsync();
      }

      public boolean isSynchronous() {
         return this.sync;
      }

   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

}
