package org.infinispan.configuration.serializing;

import static org.infinispan.configuration.serializing.SerializeUtils.writeOptional;
import static org.infinispan.configuration.serializing.SerializeUtils.writeTypedProperties;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.executors.CachedThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ScheduledThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.CustomInterceptorsConfiguration;
import org.infinispan.configuration.cache.CustomStoreConfiguration;
import org.infinispan.configuration.cache.GroupsConfiguration;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.MemoryStorageConfiguration;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.RecoveryConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.configuration.cache.StatisticsConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.configuration.global.GlobalAuthorizationConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxConfiguration;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.configuration.global.GlobalStatePathConfiguration;
import org.infinispan.configuration.global.SerializationConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.configuration.global.TemporaryGlobalStatePathConfiguration;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.global.WhiteListConfiguration;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.configuration.parsing.Parser.TransactionMode;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.mappers.ClusterRoleMapper;
import org.infinispan.security.mappers.CommonNameRoleMapper;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;

/**
 * Serializes an Infinispan configuration to an {@link XMLExtendedStreamWriter}
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class Serializer extends AbstractStoreSerializer implements ConfigurationSerializer<ConfigurationHolder> {
   private static final Map<String, Element> THREAD_POOL_FACTORIES;

   static {
      THREAD_POOL_FACTORIES = new ConcurrentHashMap<>();
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
         for (Object oProperty : properties.keySet()) {
            String property = oProperty.toString();
            if (JGroupsTransport.CHANNEL_CONFIGURATOR.equals(property)) {
               ProtocolStackConfigurator configurator = (ProtocolStackConfigurator) properties.get(property);

               if (configurator.getClass() == FileJGroupsChannelConfigurator.class) {
                  FileJGroupsChannelConfigurator fileConfigurator = (FileJGroupsChannelConfigurator) configurator;
                  writer.writeStartElement(Element.STACK_FILE);
                  writer.writeAttribute(Attribute.NAME, fileConfigurator.getName());
                  writer.writeAttribute(Attribute.PATH, fileConfigurator.getPath());
                  writer.writeEndElement();
               } else if (configurator instanceof EmbeddedJGroupsChannelConfigurator) {
                  EmbeddedJGroupsChannelConfigurator embeddedConfigurator = (EmbeddedJGroupsChannelConfigurator)configurator;
                  writer.writeStartElement(Element.STACK);
                  writer.writeAttribute(Attribute.NAME, embeddedConfigurator.getName());
                  for(ProtocolConfiguration protocol : embeddedConfigurator.getProtocolStack()) {
                     writer.writeStartElement(protocol.getProtocolName());
                     for(Entry<String, String> attr : protocol.getProperties().entrySet()) {
                        writer.writeAttribute(attr.getKey(), attr.getValue());
                     }
                     writer.writeEndElement();
                  }
                  writer.writeEndElement();
               }
            }
         }
         writer.writeEndElement();
      }
   }

   private void writeThreads(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration) throws XMLStreamException {
      writer.writeStartElement(Element.THREADS);
      ConcurrentMap<String, DefaultThreadFactory> threadFactories = new ConcurrentHashMap<>();
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
      writeThreadPool(writer, globalConfiguration.asyncThreadPoolName(), globalConfiguration.asyncThreadPool());
      writeThreadPool(writer, globalConfiguration.expirationThreadPoolName(), globalConfiguration.expirationThreadPool());
      writeThreadPool(writer, globalConfiguration.listenerThreadPoolName(), globalConfiguration.listenerThreadPool());
      writeThreadPool(writer, globalConfiguration.persistenceThreadPoolName(), globalConfiguration.persistenceThreadPool());
      writeThreadPool(writer, globalConfiguration.stateTransferThreadPoolName(), globalConfiguration.stateTransferThreadPool());
      writeThreadPool(writer, globalConfiguration.transport().remoteThreadPoolName(), globalConfiguration.transport().remoteCommandThreadPool());
      writeThreadPool(writer, globalConfiguration.transport().transportThreadPoolName(), globalConfiguration.transport().transportThreadPool());
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
         writer.writeAttribute(Attribute.NAME, globalConfiguration.cacheManagerName());
         if (globalConfiguration.shutdown().hookBehavior() != ShutdownHookBehavior.DEFAULT) {
            writer.writeAttribute(Attribute.SHUTDOWN_HOOK, globalConfiguration.shutdown().hookBehavior().name());
         }
         writer.writeAttribute(Attribute.STATISTICS, String.valueOf(globalConfiguration.statistics()));

         if (globalConfiguration.asyncThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.ASYNC_EXECUTOR, globalConfiguration.asyncThreadPoolName());
         }
         if (globalConfiguration.expirationThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.EXPIRATION_EXECUTOR, globalConfiguration.expirationThreadPoolName());
         }
         if (globalConfiguration.listenerThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.LISTENER_EXECUTOR, globalConfiguration.listenerThreadPoolName());
         }
         if (globalConfiguration.persistenceThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.PERSISTENCE_EXECUTOR, globalConfiguration.persistenceThreadPoolName());
         }
         if (globalConfiguration.stateTransferThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.STATE_TRANSFER_EXECUTOR, globalConfiguration.stateTransferThreadPoolName());
         }
         writeTransport(writer, globalConfiguration);
         writeSecurity(writer, globalConfiguration);
         writeSerialization(writer, globalConfiguration);
         writeMetrics(writer, globalConfiguration);
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
            throw CONFIG.unableToInstantiateSerializer(serializedWith.value());
         }
      }

   }

   private void writeGlobalState(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration)
         throws XMLStreamException {
      GlobalStateConfiguration configuration = globalConfiguration.globalState();
      if (configuration.enabled()) {
         writer.writeStartElement(Element.GLOBAL_STATE);

         if (configuration.persistenceConfiguration().attributes().attribute(GlobalStatePathConfiguration.PATH).isModified()) {
            writer.writeStartElement(Element.PERSISTENT_LOCATION);
            writer.writeAttribute(Attribute.PATH, configuration.persistentLocation());
            writer.writeEndElement();
         }
         if (configuration.sharedPersistenceConfiguration().attributes().attribute(GlobalStatePathConfiguration.PATH).isModified()) {
            writer.writeStartElement(Element.SHARED_PERSISTENT_LOCATION);
            writer.writeAttribute(Attribute.PATH, configuration.sharedPersistentLocation());
            writer.writeEndElement();
         }
         if (configuration.temporaryLocationConfiguration().attributes().attribute(TemporaryGlobalStatePathConfiguration.PATH).isModified()) {
            writer.writeStartElement(Element.TEMPORARY_LOCATION);
            writer.writeAttribute(Attribute.PATH, configuration.temporaryLocation());
            writer.writeEndElement();
         }
         switch (configuration.configurationStorage()) {
            case IMMUTABLE:
               writer.writeEmptyElement(Element.IMMUTABLE_CONFIGURATION_STORAGE);
               break;
            case VOLATILE:
               writer.writeEmptyElement(Element.VOLATILE_CONFIGURATION_STORAGE);
               break;
            case OVERLAY:
               writer.writeEmptyElement(Element.OVERLAY_CONFIGURATION_STORAGE);
               break;
            case MANAGED:
               writer.writeEmptyElement(Element.MANAGED_CONFIGURATION_STORAGE);
               break;
            case CUSTOM:
               writer.writeStartElement(Element.CUSTOM_CONFIGURATION_STORAGE);
               writer.writeAttribute(Attribute.CLASS, configuration.configurationStorageClass().get().getClass().getName());
               writer.writeEndElement();
               break;
         }

         writer.writeEndElement();
      }
   }

   private void writeSecurity(XMLExtendedStreamWriter writer, GlobalConfiguration configuration) throws XMLStreamException {
      GlobalAuthorizationConfiguration authorization = configuration.security().authorization();
      AttributeSet attributes = authorization.attributes();
      if (attributes.isModified() && authorization.enabled()) {
         writer.writeStartElement(Element.SECURITY);
         writer.writeStartElement(Element.AUTHORIZATION);
         attributes.write(writer, GlobalAuthorizationConfiguration.AUDIT_LOGGER, Attribute.AUDIT_LOGGER);
         PrincipalRoleMapper mapper = authorization.principalRoleMapper();
         if (mapper != null) {
            if (mapper instanceof IdentityRoleMapper) {
               writer.writeEmptyElement(Element.IDENTITY_ROLE_MAPPER);
            } else if (mapper instanceof CommonNameRoleMapper) {
               writer.writeEmptyElement(Element.COMMON_NAME_ROLE_MAPPER);
            } else if (mapper instanceof ClusterRoleMapper) {
               writer.writeEmptyElement(Element.CLUSTER_ROLE_MAPPER);
            } else {
               writer.writeStartElement(Element.CUSTOM_ROLE_MAPPER);
               writer.writeAttribute(Attribute.CLASS, mapper.getClass().getName());
               writer.writeEndElement();
            }
         }

         for(Role role : authorization.roles().values()) {
            writer.writeStartElement(Element.ROLE);
            writer.writeAttribute(Attribute.NAME, role.getName());
            writeCollectionAsAttribute(writer, Attribute.PERMISSIONS, role.getPermissions());
            writer.writeEndElement();
         }
         writer.writeEndElement();
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
         }
         attributes.write(writer, TransportConfiguration.NODE_NAME, Attribute.NODE_NAME);
         TypedProperties properties = globalConfiguration.transport().properties();
         if (properties.containsKey("stack")) {
            writer.writeAttribute(Attribute.STACK, properties.getProperty("stack"));
         }
         if (transport.remoteCommandThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.REMOTE_COMMAND_EXECUTOR, transport.remoteThreadPoolName());
         }
         if (transport.transportThreadPool().threadPoolFactory() != null) {
            writer.writeAttribute(Attribute.EXECUTOR, transport.transportThreadPoolName());
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
         SerializationConfiguration config = globalConfiguration.serialization();
         writeAdvancedSerializers(writer, config);
         writeSerializationContextInitializers(writer,config);
         writeClassWhiteList(writer, config.whiteList());
         writer.writeEndElement();
      }
   }

   private void writeAdvancedSerializers(XMLExtendedStreamWriter writer, SerializationConfiguration config) throws XMLStreamException {
      Map<Integer, AdvancedExternalizer<?>> externalizers = config.advancedExternalizers();
      boolean userExternalizerExists = externalizers.entrySet().stream().anyMatch(entry -> entry.getKey() >= AdvancedExternalizer.USER_EXT_ID_MIN);
      if (userExternalizerExists) {
         for (Entry<Integer, AdvancedExternalizer<?>> externalizer : externalizers.entrySet()) {
            writer.writeStartElement(Element.ADVANCED_EXTERNALIZER);
            writer.writeAttribute(Attribute.ID, Integer.toString(externalizer.getKey()));
            writer.writeAttribute(Attribute.CLASS, externalizer.getValue().getClass().getName());
            writer.writeEndElement();
         }
      }
   }

   private void writeSerializationContextInitializers(XMLExtendedStreamWriter writer, SerializationConfiguration config) throws XMLStreamException {
      List<SerializationContextInitializer> scis = config.contextInitializers();
      if (scis != null) {
         for (SerializationContextInitializer sci : config.contextInitializers()) {
            writer.writeStartElement(Element.SERIALIZATION_CONTEXT_INITIALIZER);
            writer.writeAttribute(Attribute.CLASS, sci.getClass().getName());
            writer.writeEndElement();
         }
      }
   }

   private void writeClassWhiteList(XMLExtendedStreamWriter writer, WhiteListConfiguration config) throws XMLStreamException {
      writer.writeStartElement(Element.WHITE_LIST);
      writeClassWhiteListElements(writer, Element.CLASS, config.getClasses());
      writeClassWhiteListElements(writer, Element.REGEX, config.getRegexps());
      writer.writeEndElement();
   }

   private void writeClassWhiteListElements(XMLExtendedStreamWriter writer, Element element, Collection<String> values) throws XMLStreamException {
      for (String value : values) {
         writer.writeStartElement(element);
         writer.writeCharacters(value);
         writer.writeEndElement();
      }
   }

   private void writeMetrics(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration) throws XMLStreamException {
      GlobalMetricsConfiguration metrics = globalConfiguration.metrics();
      AttributeSet attributes = metrics.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.METRICS);
         attributes.write(writer, GlobalMetricsConfiguration.GAUGES, Attribute.GAUGES);
         attributes.write(writer, GlobalMetricsConfiguration.HISTOGRAMS, Attribute.HISTOGRAMS);
         attributes.write(writer, GlobalMetricsConfiguration.PREFIX, Attribute.PREFIX);
         attributes.write(writer, GlobalMetricsConfiguration.NAMES_AS_TAGS, Attribute.NAMES_AS_TAGS);
         writer.writeEndElement();
      }
   }

   private void writeJMX(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration) throws XMLStreamException {
      GlobalJmxConfiguration jmx = globalConfiguration.jmx();
      AttributeSet attributes = jmx.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.JMX);
         attributes.write(writer, GlobalJmxConfiguration.ENABLED, Attribute.ENABLED);
         attributes.write(writer, GlobalJmxConfiguration.DOMAIN, Attribute.DOMAIN);
         attributes.write(writer, GlobalJmxConfiguration.ALLOW_DUPLICATE_DOMAINS, Attribute.ALLOW_DUPLICATE_DOMAINS);
         attributes.write(writer, GlobalJmxConfiguration.MBEAN_SERVER_LOOKUP, Attribute.MBEAN_SERVER_LOOKUP);
         writeTypedProperties(writer, attributes.attribute(GlobalJmxConfiguration.PROPERTIES).get());
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
      clustering.attributes().write(writer, ClusteringConfiguration.REMOTE_TIMEOUT, Attribute.REMOTE_TIMEOUT);
   }

   private void writeCommonCacheAttributesElements(XMLExtendedStreamWriter writer, String name, Configuration configuration) throws XMLStreamException {
      writer.writeAttribute(Attribute.NAME, name);
      configuration.statistics().attributes().write(writer, StatisticsConfiguration.ENABLED, Attribute.STATISTICS);
      configuration.unsafe().attributes().write(writer);
      writeBackup(writer, configuration);
      writeEncoding(writer, configuration);
      configuration.sites().backupFor().attributes().write(writer, Element.BACKUP_FOR.getLocalName());
      configuration.locking().attributes().write(writer, Element.LOCKING.getLocalName());
      writeTransaction(writer, configuration);
      configuration.expiration().attributes().write(writer, Element.EXPIRATION.getLocalName());
      writeMemory(writer, configuration);
      writePersistence(writer, configuration);
      writeIndexing(writer, configuration);
      writeCustomInterceptors(writer, configuration);
      writeSecurity(writer, configuration);
      if (configuration.clustering().cacheMode().needsStateTransfer()) {
         configuration.clustering().stateTransfer().attributes().write(writer, Element.STATE_TRANSFER.getLocalName());
      }
      writePartitionHandling(writer, configuration);
   }

   private void writeEncoding(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      MediaType keyDataType = configuration.encoding().keyDataType().mediaType();
      MediaType valueDataType = configuration.encoding().valueDataType().mediaType();
      if(keyDataType != null || valueDataType != null) {
         writer.writeStartElement(Element.ENCODING);
         if(keyDataType != null) {
            writer.writeStartElement(Element.KEY_DATA_TYPE);
            writer.writeAttribute(Attribute.MEDIA_TYPE, keyDataType.toString());
            writer.writeEndElement();
         }
         if(valueDataType != null) {
            writer.writeStartElement(Element.VALUE_DATA_TYPE);
            writer.writeAttribute(Attribute.MEDIA_TYPE, valueDataType.toString());
            writer.writeEndElement();
         }
         writer.writeEndElement();
      }
   }

   private void writePartitionHandling(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      PartitionHandlingConfiguration partitionHandling = configuration.clustering().partitionHandling();
      AttributeSet attributes = partitionHandling.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.PARTITION_HANDLING);
         attributes.write(writer, PartitionHandlingConfiguration.WHEN_SPLIT, Attribute.WHEN_SPLIT);
         EntryMergePolicy policyImpl = partitionHandling.mergePolicy();
         MergePolicy policy = MergePolicy.fromConfiguration(policyImpl);
         String output = policy == MergePolicy.CUSTOM ? policyImpl.getClass().getName() : policy.toString();
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

   private void writeMemory(XMLExtendedStreamWriter writer, Configuration configuration) throws XMLStreamException {
      MemoryConfiguration memory = configuration.memory();
      AttributeSet attributes = memory.heapConfiguration().attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.MEMORY);
         writer.writeStartElement(memory.storageType().getElement());
         switch (memory.storageType()) {
            case OFF_HEAP:
               attributes.write(writer, MemoryStorageConfiguration.ADDRESS_COUNT, Attribute.ADDRESS_COUNT);
               attributes.write(writer, MemoryStorageConfiguration.EVICTION_STRATEGY, Attribute.STRATEGY);
               // fall through
            case BINARY:
               attributes.write(writer, MemoryStorageConfiguration.EVICTION_TYPE, Attribute.EVICTION);
               // fall through
            case OBJECT:
               attributes.write(writer, MemoryStorageConfiguration.SIZE, Attribute.SIZE);
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
         attributes.write(writer, PersistenceConfiguration.AVAILABILITY_INTERVAL, Attribute.AVAILABILITY_INTERVAL);
         attributes.write(writer, PersistenceConfiguration.CONNECTION_ATTEMPTS, Attribute.CONNECTION_ATTEMPTS);
         attributes.write(writer, PersistenceConfiguration.CONNECTION_INTERVAL, Attribute.CONNECTION_INTERVAL);
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
               throw CONFIG.unableToInstantiateSerializer(serializedWith.value());
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

   private void writeCollectionAsAttribute(XMLExtendedStreamWriter writer, Attribute attribute, Collection<?> collection) throws XMLStreamException {
      if (!collection.isEmpty()) {
         StringBuilder result = new StringBuilder();
         boolean separator = false;
         for (Object item : collection) {
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
