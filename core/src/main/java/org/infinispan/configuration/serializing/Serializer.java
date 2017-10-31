package org.infinispan.configuration.serializing;

import static org.infinispan.configuration.serializing.SerializeUtils.writeOptional;
import static org.infinispan.configuration.serializing.SerializeUtils.writeTypedProperties;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;

import javax.xml.stream.XMLStreamException;

import org.infinispan.Version;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.executors.CachedThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ScheduledThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.CustomInterceptorsConfiguration;
import org.infinispan.configuration.cache.CustomStoreConfiguration;
import org.infinispan.configuration.cache.DataContainerConfiguration;
import org.infinispan.configuration.cache.GroupsConfiguration;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.JMXStatisticsConfiguration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.RecoveryConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.configuration.global.SerializationConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.Parser.MergePolicy;
import org.infinispan.configuration.parsing.Parser.TransactionMode;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Serializes an Infinispan configuration to an {@link XMLExtendedStreamWriter}
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class Serializer extends AbstractStoreSerializer implements ConfigurationSerializer<ConfigurationHolder> {
   private static final Log log = LogFactory.getLog(Serializer.class);
   private static final Map<String, Element> THREAD_POOL_FACTORIES;

   static {
      THREAD_POOL_FACTORIES = CollectionFactory.makeConcurrentMap();
      THREAD_POOL_FACTORIES.put(CachedThreadPoolExecutorFactory.class.getName(), Element.CACHED_THREAD_POOL);
      THREAD_POOL_FACTORIES.put(BlockingThreadPoolExecutorFactory.class.getName(), Element.BLOCKING_BOUNDED_QUEUE_THREAD_POOL);
      THREAD_POOL_FACTORIES.put(ScheduledThreadPoolExecutorFactory.class.getName(), Element.SCHEDULED_THREAD_POOL);
   }

   @Override
   public void serialize(XMLExtendedStreamWriter writer, ConfigurationHolder holder) throws XMLStreamException {
      GlobalConfiguration globalConfiguration = holder.getGlobalConfiguration();
      if (globalConfiguration != null) {
         writeJGroups(writer, globalConfiguration);
         writeThreads(writer, globalConfiguration);
      }
      writeCacheContainer(writer, holder);
   }

   private void writeJGroups(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration) throws XMLStreamException {
      if (globalConfiguration.isClustered()) {
         writer.writeStartElement(Element.JGROUPS);
         writer.writeAttribute(Attribute.TRANSPORT, globalConfiguration.transport().transport().getClass().getName());
         TypedProperties properties = globalConfiguration.transport().properties();
         for (String property : properties.stringPropertyNames()) {
            if (property.startsWith("stack-")) {
               String stackName = properties.getProperty(property);
               String path = properties.getProperty("stackFilePath-" + stackName);
               writer.writeStartElement(Element.STACK_FILE);
               writer.writeAttribute(Attribute.NAME, stackName);
               writer.writeAttribute(Attribute.PATH, path);
               writer.writeEndElement();
            }
         }
         writer.writeEndElement();
      }
   }

   private void writeThreads(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration) throws XMLStreamException {
      writer.writeStartElement(Element.THREADS);
      ConcurrentMap<String, DefaultThreadFactory> threadFactories = CollectionFactory.makeConcurrentMap();
      for (ThreadPoolConfiguration threadPoolConfiguration : Arrays.asList(globalConfiguration.expirationThreadPool(), globalConfiguration.listenerThreadPool(),
            globalConfiguration.persistenceThreadPool(), globalConfiguration.stateTransferThreadPool(),
            globalConfiguration.transport().remoteCommandThreadPool(), globalConfiguration.transport().transportThreadPool())) {
         ThreadFactory threadFactory = threadPoolConfiguration.threadFactory();
         if (threadFactory instanceof DefaultThreadFactory) {
            DefaultThreadFactory tf = (DefaultThreadFactory) threadFactory;
            threadFactories.putIfAbsent(tf.getName(), tf);
         }
      }
      for (DefaultThreadFactory threadFactory : threadFactories.values()) {
         writeThreadFactory(writer, threadFactory);
      }
      writeThreadPool(writer, "async-pool", globalConfiguration.asyncThreadPool());
      writeThreadPool(writer, "expiration-pool", globalConfiguration.expirationThreadPool());
      writeThreadPool(writer, "listener-pool", globalConfiguration.listenerThreadPool());
      writeThreadPool(writer, "persistence-pool", globalConfiguration.persistenceThreadPool());
      writeThreadPool(writer, "state-transfer-pool", globalConfiguration.stateTransferThreadPool());
      writeThreadPool(writer, "remote-command-pool", globalConfiguration.transport().remoteCommandThreadPool());
      writeThreadPool(writer, "transport-pool", globalConfiguration.transport().transportThreadPool());
      writer.writeEndElement();
   }

   private void writeThreadFactory(XMLExtendedStreamWriter writer, DefaultThreadFactory threadFactory) throws XMLStreamException {
      writer.writeStartElement(Element.THREAD_FACTORY);
      writeOptional(writer, Attribute.NAME, threadFactory.getName());
      writeOptional(writer, Attribute.GROUP_NAME, threadFactory.threadGroup().getName());
      writeOptional(writer, Attribute.THREAD_NAME_PATTERN, threadFactory.threadNamePattern());
      writer.writeAttribute(Attribute.PRIORITY, Integer.toString(threadFactory.initialPriority()));
      writer.writeEndElement();
   }

   private void writeThreadPool(XMLExtendedStreamWriter writer, String name, ThreadPoolConfiguration threadPoolConfiguration) throws XMLStreamException {
      ThreadPoolExecutorFactory<?> threadPoolFactory = threadPoolConfiguration.threadPoolFactory();
      if (threadPoolFactory != null) {
         writer.writeStartElement(THREAD_POOL_FACTORIES.get(threadPoolFactory.getClass().getName()));
         writer.writeAttribute(Attribute.NAME, name);
         ThreadFactory threadFactory = threadPoolConfiguration.threadFactory();
         if (threadFactory instanceof DefaultThreadFactory) {
            DefaultThreadFactory tf = (DefaultThreadFactory) threadFactory;
            writer.writeAttribute(Attribute.THREAD_FACTORY, tf.getName());
         }
         if (threadPoolFactory instanceof BlockingThreadPoolExecutorFactory) {
            BlockingThreadPoolExecutorFactory pool = (BlockingThreadPoolExecutorFactory) threadPoolFactory;
            writer.writeAttribute(Attribute.MAX_THREADS, Integer.toString(pool.maxThreads()));
            writer.writeAttribute(Attribute.CORE_THREADS, Integer.toString(pool.coreThreads()));
            writer.writeAttribute(Attribute.QUEUE_LENGTH, Integer.toString(pool.queueLength()));
            writer.writeAttribute(Attribute.KEEP_ALIVE_TIME, Long.toString(pool.keepAlive()));
         }
         writer.writeEndElement();
      }
   }

   private void writeCacheContainer(XMLExtendedStreamWriter writer, ConfigurationHolder holder) throws XMLStreamException {
      writer.writeStartElement(Element.CACHE_CONTAINER);
      GlobalConfiguration globalConfiguration = holder.getGlobalConfiguration();
      if (globalConfiguration != null) {
         writer.writeAttribute(Attribute.NAME, globalConfiguration.globalJmxStatistics().cacheManagerName());
         if (globalConfiguration.shutdown().hookBehavior() != ShutdownHookBehavior.DEFAULT) {
            writer.writeAttribute(Attribute.SHUTDOWN_HOOK, globalConfiguration.shutdown().hookBehavior().name());
         }
         globalConfiguration.globalJmxStatistics().attributes().write(writer, GlobalJmxStatisticsConfiguration.ENABLED, Attribute.STATISTICS);
         if (globalConfiguration.asyncThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.ASYNC_EXECUTOR, "async-pool");
         }
         if (globalConfiguration.expirationThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.EXPIRATION_EXECUTOR, "expiration-pool");
         }
         if (globalConfiguration.listenerThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.LISTENER_EXECUTOR, "listener-pool");
         }
         if (globalConfiguration.persistenceThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.PERSISTENCE_EXECUTOR, "persistence-pool");
         }
         if (globalConfiguration.stateTransferThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.STATE_TRANSFER_EXECUTOR, "state-transfer-pool");
         }
         writeTransport(writer, globalConfiguration);
         writeSerialization(writer, globalConfiguration);
         writeJMX(writer, globalConfiguration);
         writeGlobalState(writer, globalConfiguration);
         writeExtraConfiguration(writer, globalConfiguration.modules());
      }
      for (Entry<String, Configuration> configuration : holder.getConfigurations().entrySet()) {
         Configuration config = configuration.getValue();
         switch (config.clustering().cacheMode()) {
            case LOCAL:
               writeLocalCache(writer, configuration.getKey(), config);
               break;
            case DIST_ASYNC:
            case DIST_SYNC:
               writeDistributedCache(writer, configuration.getKey(), config);
               break;
            case INVALIDATION_ASYNC:
            case INVALIDATION_SYNC:
               writeInvalidationCache(writer, configuration.getKey(), config);
               break;
            case REPL_ASYNC:
            case REPL_SYNC:
               writeReplicatedCache(writer, configuration.getKey(), config);
               break;
            default:
               break;
         }
         writeExtraConfiguration(writer, config.modules());
      }
   }

   private void writeExtraConfiguration(XMLExtendedStreamWriter writer, Map<Class<?>, ?> modules)
         throws XMLStreamException {
      for (Entry<Class<?>, ?> entry : modules.entrySet()) {
         SerializedWith serializedWith = entry.getKey().getAnnotation(SerializedWith.class);
         if (serializedWith == null) {
            continue;
         }
         try {
            ConfigurationSerializer<Object> serializer = Util.getInstanceStrict(serializedWith.value());
            serializer.serialize(writer, entry.getValue());
         } catch (InstantiationException | IllegalAccessException e) {
            throw log.unableToInstantiateSerializer(serializedWith.value());
         }
      }

   }

   private void writeGlobalState(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration)
         throws XMLStreamException {
      GlobalStateConfiguration configuration = globalConfiguration.globalState();
      if (configuration.enabled()) {
         writer.writeStartElement(Element.GLOBAL_STATE);
         if (configuration.attributes().attribute(GlobalStateConfiguration.PERSISTENT_LOCATION).isModified()) {
            writer.writeStartElement(Element.PERSISTENT_LOCATION);
            writer.writeAttribute(Attribute.PATH, configuration.persistentLocation());
            writer.writeEndElement();
         }
         if (configuration.attributes().attribute(GlobalStateConfiguration.TEMPORARY_LOCATION).isModified()) {
            writer.writeStartElement(Element.TEMPORARY_LOCATION);
            writer.writeAttribute(Attribute.PATH, configuration.temporaryLocation());
            writer.writeEndElement();
         }
         writer.writeEndElement();
      }
   }

   private void writeReplicatedCache(XMLExtendedStreamWriter writer, String name, Configuration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.REPLICATED_CACHE);
      writeCommonClusteredCacheAttributes(writer, configuration);
      writeCommonCacheAttributesElements(writer, name, configuration);
      writer.writeEndElement();
   }

   private void writeDistributedCache(XMLExtendedStreamWriter writer, String name, Configuration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.DISTRIBUTED_CACHE);
      configuration.clustering().hash().attributes().write(writer);
      configuration.clustering().l1().attributes().write(writer);
      writeCommonClusteredCacheAttributes(writer, configuration);
      writeCommonCacheAttributesElements(writer, name, configuration);
      GroupsConfiguration groups = configuration.clustering().hash().groups();
      if (groups.attributes().isModified()) {
         writer.writeStartElement(Element.GROUPS);
         groups.attributes().write(writer, GroupsConfiguration.ENABLED);
         for (Grouper<?> grouper : groups.groupers()) {
            writer.writeStartElement(Element.GROUPER);
            writer.writeAttribute(Attribute.CLASS, grouper.getClass().getName());
            writer.writeEndElement();
         }
         writer.writeEndElement();
      }
      writer.writeEndElement();
   }

   private void writeInvalidationCache(XMLExtendedStreamWriter writer, String name, Configuration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.INVALIDATION_CACHE);
      writeCommonClusteredCacheAttributes(writer, configuration);
      writeCommonCacheAttributesElements(writer, name, configuration);
      writer.writeEndElement();
   }

   private void writeLocalCache(XMLExtendedStreamWriter writer, String name, Configuration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.LOCAL_CACHE);
      if (configuration.simpleCache()) {
         configuration.attributes().write(writer, Configuration.SIMPLE_CACHE, Attribute.SIMPLE_CACHE);
      }
      writeCommonCacheAttributesElements(writer, name, configuration);
      writer.writeEndElement();
   }

   private void writeTransport(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration) throws XMLStreamException {
      TransportConfiguration transport = globalConfiguration.transport();
      AttributeSet attributes = transport.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.TRANSPORT);
         attributes.write(writer, TransportConfiguration.CLUSTER_NAME, Attribute.CLUSTER);
         attributes.write(writer, TransportConfiguration.MACHINE_ID, Attribute.MACHINE_ID);
         attributes.write(writer, TransportConfiguration.RACK_ID, Attribute.RACK_ID);
         if (transport.siteId() != null) {
            attributes.write(writer, TransportConfiguration.SITE_ID, Attribute.SITE);
         } else if (globalConfiguration.sites().localSite() != null) {
            writer.writeAttribute(Attribute.SITE, globalConfiguration.sites().localSite());
         }
         attributes.write(writer, TransportConfiguration.NODE_NAME, Attribute.NODE_NAME);
         TypedProperties properties = globalConfiguration.transport().properties();
         if (properties.containsKey("stack")) {
            writer.writeAttribute(Attribute.STACK, properties.getProperty("stack"));
         }
         if (transport.remoteCommandThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.REMOTE_COMMAND_EXECUTOR, "remote-command-pool");
         }
         if (transport.transportThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.EXECUTOR, "transport-pool");
         }
         attributes.write(writer, TransportConfiguration.DISTRIBUTED_SYNC_TIMEOUT, Attribute.LOCK_TIMEOUT);
         attributes.write(writer, TransportConfiguration.INITIAL_CLUSTER_SIZE, Attribute.INITIAL_CLUSTER_SIZE);
         attributes.write(writer, TransportConfiguration.INITIAL_CLUSTER_TIMEOUT, Attribute.INITIAL_CLUSTER_TIMEOUT);
         writer.writeEndElement();
      }
   }

   private void writeSerialization(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration) throws XMLStreamException {
      SerializationConfiguration serialization = globalConfiguration.serialization();
      AttributeSet attributes = serialization.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.SERIALIZATION);
         attributes.write(writer, SerializationConfiguration.MARSHALLER, Attribute.MARSHALLER_CLASS);
         if (attributes.attribute(SerializationConfiguration.VERSION).isModified()) {
            writer.writeAttribute(Attribute.VERSION, Version.decodeVersion(serialization.version()));
         }
         writeAdvancedSerializers(writer, globalConfiguration);
         writer.writeEndElement();
      }
   }

   private void writeAdvancedSerializers(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration) throws XMLStreamException {
      Map<Integer, AdvancedExternalizer<?>> externalizers = globalConfiguration.serialization().advancedExternalizers();
      for (Entry<Integer, AdvancedExternalizer<?>> externalizer : externalizers.entrySet()) {
         writer.writeStartElement(Element.ADVANCED_EXTERNALIZER);
         writer.writeAttribute(Attribute.ID, Integer.toString(externalizer.getKey()));
         writer.writeAttribute(Attribute.CLASS, externalizer.getValue().getClass().getName());
         writer.writeEndElement();
      }
   }

   private void writeJMX(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration) throws XMLStreamException {
      GlobalJmxStatisticsConfiguration globalJmxStatistics = globalConfiguration.globalJmxStatistics();
      AttributeSet attributes = globalJmxStatistics.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.JMX);
         attributes.write(writer, GlobalJmxStatisticsConfiguration.JMX_DOMAIN, Attribute.JMX_DOMAIN);
         attributes.write(writer, GlobalJmxStatisticsConfiguration.ALLOW_DUPLICATE_DOMAINS, Attribute.ALLOW_DUPLICATE_DOMAINS);
         attributes.write(writer, GlobalJmxStatisticsConfiguration.MBEAN_SERVER_LOOKUP, Attribute.MBEAN_SERVER_LOOKUP);
         writeTypedProperties(writer, attributes.attribute(GlobalJmxStatisticsConfiguration.PROPERTIES).get());
         writer.writeEndElement();
      }
   }

   private void writeTransaction(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      TransactionConfiguration transaction = configuration.transaction();
      AttributeSet attributes = transaction.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.TRANSACTION);
         TransactionMode mode = TransactionMode.fromConfiguration(transaction.transactionMode(), !transaction.useSynchronization(), transaction.recovery().enabled(), configuration
               .invocationBatching().enabled());
         writer.writeAttribute(Attribute.MODE, mode.toString());
         attributes.write(writer);
         if (mode != TransactionMode.NONE) {
            attributes.write(writer, TransactionConfiguration.TRANSACTION_MANAGER_LOOKUP);
         }
         if (transaction.recovery().enabled())
            transaction.recovery().attributes().write(writer, RecoveryConfiguration.RECOVERY_INFO_CACHE_NAME, Attribute.RECOVERY_INFO_CACHE_NAME);
         writer.writeEndElement();
      }
   }

   private void writeSecurity(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      AuthorizationConfiguration authorization = configuration.security().authorization();
      AttributeSet attributes = authorization.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.SECURITY);
         writer.writeStartElement(Element.AUTHORIZATION);
         attributes.write(writer, AuthorizationConfiguration.ENABLED, Attribute.ENABLED);
         writeCollectionAsAttribute(writer, Attribute.ROLES, authorization.roles());
         writer.writeEndElement();
         writer.writeEndElement();
      }
   }

   private void writeCommonClusteredCacheAttributes(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      ClusteringConfiguration clustering = configuration.clustering();
      writer.writeAttribute(Attribute.MODE, clustering.cacheMode().isSynchronous() ? "SYNC" : "ASYNC");
      writer.writeAttribute(Attribute.REMOTE_TIMEOUT, Long.toString(clustering.remoteTimeout()));
   }

   private void writeCommonCacheAttributesElements(XMLExtendedStreamWriter writer, String name, Configuration configuration) throws XMLStreamException {
      writer.writeAttribute(Attribute.NAME, name);
      configuration.jmxStatistics().attributes().write(writer, JMXStatisticsConfiguration.ENABLED, Attribute.STATISTICS);
      configuration.unsafe().attributes().write(writer);
      writeBackup(writer, configuration);
      configuration.sites().backupFor().attributes().write(writer, Element.BACKUP_FOR.getLocalName());
      configuration.locking().attributes().write(writer, Element.LOCKING.getLocalName());
      writeTransaction(writer, configuration);
      configuration.expiration().attributes().write(writer, Element.EXPIRATION.getLocalName());
      if (configuration.compatibility().enabled())
         configuration.compatibility().attributes().write(writer, Element.COMPATIBILITY.getLocalName());
      writeMemory(writer, configuration);
      writePersistence(writer, configuration);
      writeDataContainer(writer, configuration);
      writeIndexing(writer, configuration);
      writeCustomInterceptors(writer, configuration);
      writeSecurity(writer, configuration);
      if (configuration.clustering().cacheMode().needsStateTransfer()) {
         configuration.clustering().stateTransfer().attributes().write(writer, Element.STATE_TRANSFER.getLocalName());
      }
      writePartitionHandling(writer, configuration);
   }

   private void writePartitionHandling(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      PartitionHandlingConfiguration partitionHandling = configuration.clustering().partitionHandling();
      AttributeSet attributes = partitionHandling.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.PARTITION_HANDLING);
         attributes.write(writer, PartitionHandlingConfiguration.WHEN_SPLIT, Attribute.WHEN_SPLIT);
         EntryMergePolicy policyImpl = partitionHandling.mergePolicy();
         MergePolicy policy = MergePolicy.fromConfiguration(policyImpl);
         String output = policy == Parser.MergePolicy.CUSTOM ? policyImpl.getClass().getName() : policy.toString();
         writer.writeAttribute(Attribute.MERGE_POLICY, output);
         writer.writeEndElement();
      }
   }

   private void writeCustomInterceptors(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      CustomInterceptorsConfiguration customInterceptors = configuration.customInterceptors();
      if (customInterceptors.interceptors().size() > 0) {
         writer.writeStartElement(Element.CUSTOM_INTERCEPTORS);
         for (InterceptorConfiguration interceptor : customInterceptors.interceptors()) {
            AttributeSet attributes = interceptor.attributes();
            if (!attributes.attribute(InterceptorConfiguration.INTERCEPTOR_CLASS).isNull()) {
               writer.writeStartElement(Element.INTERCEPTOR);
               attributes.write(writer, InterceptorConfiguration.INTERCEPTOR_CLASS, Attribute.CLASS);
               attributes.write(writer, InterceptorConfiguration.AFTER, Attribute.AFTER);
               attributes.write(writer, InterceptorConfiguration.BEFORE, Attribute.BEFORE);
               attributes.write(writer, InterceptorConfiguration.INDEX, Attribute.INDEX);
               attributes.write(writer, InterceptorConfiguration.POSITION, Attribute.POSITION);
               writeTypedProperties(writer, interceptor.properties());
               writer.writeEndElement();
            }
         }
         writer.writeEndElement();
      }
   }

   private void writeDataContainer(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      DataContainerConfiguration dataContainer = configuration.dataContainer();
      AttributeSet attributes = dataContainer.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.DATA_CONTAINER);
         attributes.write(writer, DataContainerConfiguration.DATA_CONTAINER, Attribute.CLASS);
         writeTypedProperties(writer, dataContainer.properties());
         writer.writeEndElement();
      }
   }

   private void writeMemory(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      MemoryConfiguration memory = configuration.memory();
      AttributeSet attributes = memory.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.MEMORY);
         writer.writeStartElement(memory.storageType().getElement());
         switch (memory.storageType()) {
            case OFF_HEAP:
               attributes.write(writer, MemoryConfiguration.ADDRESS_COUNT, Attribute.ADDRESS_COUNT);
            case BINARY:
               attributes.write(writer, MemoryConfiguration.EVICTION_TYPE, Attribute.EVICTION);
            case OBJECT:
               attributes.write(writer, MemoryConfiguration.SIZE, Attribute.SIZE);
         }
         writer.writeEndElement();
         writer.writeEndElement();
      }
   }

   private void writeIndexing(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      IndexingConfiguration indexing = configuration.indexing();
      AttributeSet attributes = indexing.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.INDEXING);
         attributes.write(writer, IndexingConfiguration.INDEX, Attribute.INDEX);
         attributes.write(writer, IndexingConfiguration.AUTO_CONFIG, Attribute.AUTO_CONFIG);
         writeTypedProperties(writer, indexing.properties());
         writer.writeEndElement();
      }
   }

   private void writePersistence(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      PersistenceConfiguration persistence = configuration.persistence();
      AttributeSet attributes = persistence.attributes();
      if (attributes.isModified() || persistence.stores().size() > 0) {
         writer.writeStartElement(Element.PERSISTENCE);
         attributes.write(writer, PersistenceConfiguration.PASSIVATION, Attribute.PASSIVATION);
         for (StoreConfiguration store : persistence.stores()) {
            writeStore(writer, store);
         }
         writer.writeEndElement();
      }
   }

   private void writeStore(XMLExtendedStreamWriter writer, StoreConfiguration configuration) throws XMLStreamException {
      if (configuration instanceof SingleFileStoreConfiguration) {
         writeFileStore(writer, (SingleFileStoreConfiguration) configuration);
      } else if (configuration instanceof ClusterLoaderConfiguration) {
         writeClusterLoader(writer, (ClusterLoaderConfiguration) configuration);
      } else if (configuration instanceof CustomStoreConfiguration) {
         writeCustomStore(writer, (CustomStoreConfiguration) configuration);
      } else {
         SerializedWith serializedWith = configuration.getClass().getAnnotation(SerializedWith.class);
         if (serializedWith == null) {
            ConfigurationFor configurationFor = configuration.getClass().getAnnotation(ConfigurationFor.class);
            if (configuration instanceof AbstractStoreConfiguration && configurationFor != null) {
               writer.writeComment("A serializer for the store configuration class " + configuration.getClass().getName() + " was not found. Using custom store mode");
               AbstractStoreConfiguration asc = (AbstractStoreConfiguration) configuration;
               writeGenericStore(writer, configurationFor.value().getName(), asc);
            } else
               throw new UnsupportedOperationException("Cannot serialize store configuration " + configuration.getClass().getName());
         } else {
            ConfigurationSerializer<StoreConfiguration> serializer;
            try {
               serializer = Util.getInstanceStrict(serializedWith.value());
               serializer.serialize(writer, configuration);
            } catch (Exception e) {
               throw log.unableToInstantiateSerializer(serializedWith.value());
            }
         }
      }
   }

   private void writeBackup(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      SitesConfiguration sites = configuration.sites();
      if (sites.allBackups().size() > 0) {
         writer.writeStartElement(Element.BACKUPS);
         for (BackupConfiguration backup : sites.allBackups()) {
            writer.writeStartElement(Element.BACKUP);
            backup.attributes().write(writer);
            AttributeSet stateTransfer = backup.stateTransfer().attributes();
            if (stateTransfer.isModified()) {
               writer.writeStartElement(Element.STATE_TRANSFER);
               stateTransfer.write(writer, XSiteStateTransferConfiguration.CHUNK_SIZE, Attribute.CHUNK_SIZE);
               stateTransfer.write(writer, XSiteStateTransferConfiguration.MAX_RETRIES, Attribute.MAX_RETRIES);
               stateTransfer.write(writer, XSiteStateTransferConfiguration.TIMEOUT, Attribute.TIMEOUT);
               stateTransfer.write(writer, XSiteStateTransferConfiguration.WAIT_TIME, Attribute.WAIT_TIME);
               writer.writeEndElement();
            }
            AttributeSet takeOffline = backup.takeOffline().attributes();
            if (takeOffline.isModified()) {
               writer.writeStartElement(Element.TAKE_OFFLINE);
               takeOffline.write(writer, TakeOfflineConfiguration.AFTER_FAILURES, Attribute.TAKE_BACKUP_OFFLINE_AFTER_FAILURES);
               takeOffline.write(writer, TakeOfflineConfiguration.MIN_TIME_TO_WAIT, Attribute.TAKE_BACKUP_OFFLINE_MIN_WAIT);
               writer.writeEndElement();
            }
            writer.writeEndElement();
         }
         writer.writeEndElement();
      }
   }

   private void writeCollectionAsAttribute(XMLExtendedStreamWriter writer, Attribute attribute, Collection<String> collection) throws XMLStreamException {
      if (!collection.isEmpty()) {
         StringBuilder result = new StringBuilder();
         boolean separator = false;
         for (String item : collection) {
            if (separator)
               result.append(" ");
            result.append(item);
            separator = true;
         }
         writer.writeAttribute(attribute, result.toString());
      }
   }

   private void writeFileStore(XMLExtendedStreamWriter writer, SingleFileStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.FILE_STORE);
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   private void writeClusterLoader(XMLExtendedStreamWriter writer, ClusterLoaderConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.CLUSTER_LOADER);
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   private void writeCustomStore(XMLExtendedStreamWriter writer, CustomStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.STORE);
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   private void writeGenericStore(XMLExtendedStreamWriter writer, String storeClassName, AbstractStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.STORE);
      writer.writeAttribute(Attribute.CLASS.getLocalName(), storeClassName);
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }
}
