package org.infinispan.configuration.parsing;

import static org.infinispan.configuration.parsing.ParseUtils.ignoreAttribute;
import static org.infinispan.configuration.parsing.ParseUtils.ignoreElement;
import static org.infinispan.configuration.parsing.Parser.NAMESPACE;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.GlobUtils;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.ClusteringConfigurationBuilder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ContentTypeConfigurationBuilder;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.infinispan.configuration.cache.EncodingConfigurationBuilder;
import org.infinispan.configuration.cache.GroupsConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.IndexMergeConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.IndexWriterConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryConfigurationBuilder;
import org.infinispan.configuration.cache.PartitionHandlingConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SecurityConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.expiration.TouchMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.persistence.cluster.ClusterLoader;
import org.infinispan.persistence.file.SingleFileStore;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.xsite.spi.XSiteMergePolicy;
import org.kohsuke.MetaInfServices;

/**
 * This class implements the parser for Infinispan/AS7/EAP/JDG schema files
 *
 * @author Tristan Tarrant
 * @author Galder Zamarreño
 * @since 12.0
 */
@MetaInfServices(ConfigurationParser.class)
@Namespace(root = "local-cache")
@Namespace(root = "distributed-cache")
@Namespace(root = "replicated-cache")
@Namespace(root = "scattered-cache")
@Namespace(uri = NAMESPACE + "*", root = "local-cache")
@Namespace(uri = NAMESPACE + "*", root = "distributed-cache")
@Namespace(uri = NAMESPACE + "*", root = "replicated-cache")
@Namespace(uri = NAMESPACE + "*", root = "scattered-cache")
public class CacheParser implements ConfigurationParser {
   public static final String NAMESPACE = "urn:infinispan:config:";

   public CacheParser() {
   }

   @Override
   public void readElement(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      Element element = Element.forName(reader.getLocalName());
      String name = reader.getAttributeValue(Attribute.NAME.getLocalName());
      switch (element) {
         case LOCAL_CACHE: {
            parseLocalCache(reader, holder, name,false);
            break;
         }
         case LOCAL_CACHE_CONFIGURATION: {
            parseLocalCache(reader, holder, name, true);
            break;
         }
         case INVALIDATION_CACHE: {
            parseInvalidationCache(reader, holder, name,false);
            break;
         }
         case INVALIDATION_CACHE_CONFIGURATION: {
            parseInvalidationCache(reader, holder, name, true);
            break;
         }
         case REPLICATED_CACHE: {
            parseReplicatedCache(reader, holder, name, false);
            break;
         }
         case REPLICATED_CACHE_CONFIGURATION: {
            parseReplicatedCache(reader, holder, name, true);
            break;
         }
         case DISTRIBUTED_CACHE: {
            parseDistributedCache(reader, holder, name, false);
            break;
         }
         case DISTRIBUTED_CACHE_CONFIGURATION: {
            parseDistributedCache(reader, holder, name, true);
            break;
         }
         case SCATTERED_CACHE: {
            if (reader.getSchema().since(9, 1)) {
               parseScatteredCache(reader, holder, name,false);
            } else {
               throw ParseUtils.unexpectedElement(reader);
            }
            break;
         }
         case SCATTERED_CACHE_CONFIGURATION: {
            if (reader.getSchema().since(9, 1)) {
               parseScatteredCache(reader, holder, name, true);
            } else {
               throw ParseUtils.unexpectedElement(reader);
            }
            break;
         }
      }
   }

   private void parseModules(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      while (reader.inTag()) {
         reader.handleAny(holder);
      }
   }

   protected void parseLocalCache(ConfigurationReader reader, ConfigurationBuilderHolder holder, String name, boolean template) {
      holder.pushScope(template ? ParserScope.CACHE_TEMPLATE : ParserScope.CACHE);
      if (!template && GlobUtils.isGlob(name))
         throw CONFIG.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      builder.clustering().cacheMode(CacheMode.LOCAL);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         this.parseCacheAttribute(reader, i, attribute, value, builder);
      }

      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         this.parseCacheElement(reader, element, holder);
      }
      holder.popScope();
   }

   private void parseCacheAttribute(ConfigurationReader reader,
         int index, Attribute attribute, String value, ConfigurationBuilder builder) {
      switch (attribute) {
         case NAME:
         case CONFIGURATION:
            // Handled by the caller
            break;
         case START:
         case JNDI_NAME:
         case MODULE: {
            ignoreAttribute(reader, index);
            break;
         }
         case SIMPLE_CACHE:
            builder.simpleCache(Boolean.parseBoolean(value));
            break;
         case STATISTICS: {
            builder.statistics().enabled(Boolean.parseBoolean(value));
            break;
         }
         case STATISTICS_AVAILABLE: {
            builder.statistics().available(Boolean.parseBoolean(value));
            break;
         }
         case SPIN_DURATION: {
            if (reader.getSchema().since(10, 0)) {
               throw ParseUtils.attributeRemoved(reader, index);
            } else {
               ignoreAttribute(reader, index);
            }
            break;
         }
         case UNRELIABLE_RETURN_VALUES: {
            builder.unsafe().unreliableReturnValues(Boolean.parseBoolean(value));
            break;
         }
         default: {
            if (ParseUtils.isNoNamespaceAttribute(reader, index)) {
               throw ParseUtils.unexpectedAttribute(reader, index);
            }
         }
      }
   }

   private void parseSharedStateCacheElement(ConfigurationReader reader, Element element, ConfigurationBuilderHolder holder) {
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

   private void parseBackups(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      // If backups is present then remove any existing backups as they were added by the default config.
      builder.sites().backups().clear();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case MAX_CLEANUP_DELAY:
               builder.sites().maxTombstoneCleanupDelay(Long.parseLong(value));
               break;
            case TOMBSTONE_MAP_SIZE:
               builder.sites().tombstoneMapSize(Integer.parseInt(value));
               break;
            case MERGE_POLICY:
               builder.sites().mergePolicy(XSiteMergePolicy.instanceFromString(value, holder.getClassLoader()));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.inTag()) {
         Map.Entry<String, String> item = reader.getMapItem(Attribute.SITE);
         Element element = Element.forName(item.getValue());
         if (element == Element.BACKUP) {
            this.parseBackup(reader, builder, item.getKey());
         } else {
            throw ParseUtils.unexpectedElement(reader);
         }
         reader.endMapItem();
      }
   }

   private void parsePartitionHandling(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      PartitionHandlingConfigurationBuilder ph = builder.clustering().partitionHandling();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case ENABLED: {
               if (reader.getSchema().since(11, 0))
                  throw ParseUtils.attributeRemoved(reader, i, Attribute.WHEN_SPLIT.getLocalName());
               ignoreAttribute(reader, i);
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

   private void parseBackup(ConfigurationReader reader, ConfigurationBuilder builder, String site) {
      BackupConfigurationBuilder backup = builder.sites().addBackup().site(site);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case SITE: {
               // Already seen
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
               backup.replicationTimeout(Long.parseLong(value));
               break;
            }
            case ENABLED: {
               CONFIG.configDeprecated(attribute);
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

      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case TAKE_OFFLINE: {
               parseTakeOffline(reader, backup);
               break;
            }
            case STATE_TRANSFER: {
               parseXSiteStateTransfer(reader, backup);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseTakeOffline(ConfigurationReader reader, BackupConfigurationBuilder backup) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case TAKE_BACKUP_OFFLINE_AFTER_FAILURES: {
               backup.takeOffline().afterFailures(Integer.parseInt(value));
               break;
            }
            case TAKE_BACKUP_OFFLINE_MIN_WAIT: {
               backup.takeOffline().minTimeToWait(Long.parseLong(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseXSiteStateTransfer(ConfigurationReader reader, BackupConfigurationBuilder backup) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
            case MODE:
               backup.stateTransfer().mode(XSiteStateTransferMode.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseBackupFor(ConfigurationReader reader, ConfigurationBuilder builder) {
      builder.sites().backupFor().reset();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

   private void parseCacheSecurity(ConfigurationReader reader, ConfigurationBuilder builder) {
      SecurityConfigurationBuilder securityBuilder = builder.security();
      ParseUtils.requireNoAttributes(reader);
      while (reader.inTag()) {
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

   private void parseCacheAuthorization(ConfigurationReader reader, AuthorizationConfigurationBuilder authzBuilder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case ENABLED: {
               authzBuilder.enabled(Boolean.parseBoolean(reader.getAttributeValue(i)));
               break;
            }
            case ROLES: {
               for(String role : reader.getListAttributeValue(i)) {
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

   protected final void parseCacheElement(ConfigurationReader reader, Element element, ConfigurationBuilderHolder holder) {
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
            if (reader.getSchema().since(10, 0)) {
               throw ParseUtils.elementRemoved(reader, Element.MEMORY.getLocalName());
            } else {
               this.parseEviction(reader, builder);
            }
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
            CONFIG.customInterceptorsDeprecated();
            this.parseCustomInterceptors(reader, holder);
            break;
         }
         case VERSIONING: {
            parseVersioning(reader, holder);
            break;
         }
         case COMPATIBILITY: {
            if (!reader.getSchema().since(10, 0)) {
               parseCompatibility(reader, holder);
            }
            break;
         }
         case STORE_AS_BINARY: {
            parseStoreAsBinary(reader, holder);
            break;
         }
         case MODULES: {
            if (reader.getSchema().since(9, 0)) {
               throw ParseUtils.elementRemoved(reader);
            } else {
               parseModules(reader, holder);
            }
            break;
         }
         case DATA_CONTAINER: {
            if (reader.getSchema().since(10, 0)) {
               throw ParseUtils.elementRemoved(reader);
            } else {
               parseDataContainer(reader);
            }
            break;
         }
         case MEMORY: {
            parseMemory(reader, holder);
            break;
         }
         case BACKUPS: {
            this.parseBackups(reader, holder);
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

   private void parseDataContainer(final ConfigurationReader reader) {
      ignoreElement(reader, Element.DATA_CONTAINER);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case CLASS:
            case KEY_EQUIVALENCE:
            case VALUE_EQUIVALENCE:
               ignoreAttribute(reader, i);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      Properties properties = new Properties();
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PROPERTY: {
               ignoreElement(reader, element);
               parseProperty(reader, properties);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseMemory(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      if (reader.getSchema().since(11, 0)) {
         for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeName(i));
            switch (attribute) {
               case STORAGE:
                  memoryBuilder.storage(StorageType.valueOf(value));
                  break;
               case MAX_SIZE:
                  memoryBuilder.maxSize(value);
                  break;
               case MAX_COUNT:
                  memoryBuilder.maxCount(Long.parseLong(value));
                  break;
               case WHEN_FULL:
                  memoryBuilder.whenFull(EvictionStrategy.valueOf(value));
                  break;
               default:
                  throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         CONFIG.warnUsingDeprecatedMemoryConfigs(element.getLocalName());
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

   private void parseOffHeapMemoryAttributes(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case SIZE:
               memoryBuilder.size(Long.parseLong(value));
               break;
            case EVICTION:
               memoryBuilder.evictionType(EvictionType.valueOf(value));
               break;
            case ADDRESS_COUNT:
               if (reader.getSchema().since(10, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ignoreAttribute(reader, i);
               }
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

   private void parseObjectMemoryAttributes(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

   private void parseBinaryMemoryAttributes(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

   private void parseStoreAsBinary(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      CONFIG.configDeprecatedUseOther(Element.STORE_AS_BINARY, Element.MEMORY);
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Boolean binaryKeys = null;
      Boolean binaryValues = null;
      builder.memory().storageType(StorageType.BINARY);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

   private void parseCompatibility(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      EncodingConfigurationBuilder encoding = builder.encoding();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  encoding.key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
                  encoding.value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
               }
               break;
            case MARSHALLER:
               CONFIG.marshallersNotSupported();
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseVersioning(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case VERSIONING_SCHEME:
               if (reader.getSchema().since(10, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  CONFIG.ignoredAttribute("versioning", "9.0", attribute.getLocalName(), reader.getLocation().getLineNumber());
               }
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseCustomInterceptors(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      ParseUtils.requireNoAttributes(reader);
      while (reader.inTag()) {
         Map.Entry<String, String> item = reader.getMapItem(Attribute.CLASS);
         Element element = Element.forName(item.getValue());
         switch (element) {
            case INTERCEPTOR: {
               parseInterceptor(reader, holder, item.getKey());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
         reader.endMapItem();
      }
   }

   private void parseInterceptor(ConfigurationReader reader, ConfigurationBuilderHolder holder, String klass) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();
      interceptorBuilder.interceptorClass(Util.loadClass(klass, holder.getClassLoader()));
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case AFTER:
               interceptorBuilder.after(Util.loadClass(value, holder.getClassLoader()));
               break;
            case BEFORE:
               interceptorBuilder.before(Util.loadClass(value, holder.getClassLoader()));
               break;
            case CLASS:
               // Already seen
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

      interceptorBuilder.withProperties(parseProperties(reader, Element.INTERCEPTOR));
   }

   private final void parseLocking(ConfigurationReader reader, ConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
               CONFIG.ignoredAttribute("write skew attribute", "9.0", attribute.getLocalName(), reader.getLocation().getLineNumber());
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private final void parseTransaction(ConfigurationReader reader, ConfigurationBuilder builder, ConfigurationBuilderHolder holder) {
      if (!reader.getSchema().since(9, 0)) {
         CacheMode cacheMode = builder.clustering().cacheMode();
         if (!cacheMode.isSynchronous()) {
            CONFIG.unsupportedAsyncCacheMode(cacheMode, cacheMode.toSync());
            builder.clustering().cacheMode(cacheMode.toSync());
         }
      }
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
               if (reader.getSchema().since(11, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  CONFIG.ignoredAttribute("transaction protocol", "11.0", attribute.getLocalName(), reader.getLocation().getLineNumber());
               }
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

   private final void parseDataType(ConfigurationReader reader, ConfigurationBuilder builder, ConfigurationBuilderHolder holder) {
      EncodingConfigurationBuilder encodingBuilder = builder.encoding();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         if (attribute == Attribute.MEDIA_TYPE && reader.getSchema().since(11, 0)) {
            encodingBuilder.mediaType(value);
         } else {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }

      }
      while (reader.inTag()) {
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

   private void parseContentType(ConfigurationReader reader, ConfigurationBuilderHolder holder, ContentTypeConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case MEDIA_TYPE:
               builder.mediaType(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
   }

   private void parseEviction(ConfigurationReader reader, ConfigurationBuilder builder) {
      CONFIG.configDeprecatedUseOther(Element.EVICTION, Element.MEMORY);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case STRATEGY:
            case THREAD_POLICY:
            case TYPE:
               ignoreAttribute(reader, i);
               break;
            case MAX_ENTRIES:
            case SIZE:
               long size = Long.parseLong(value);
               if (size >= 0) {
                  builder.memory().size(size);
               }
               break;
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseExpiration(ConfigurationReader reader, ConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
            case TOUCH: {
               if (reader.getSchema().since(12, 1)) {
                  builder.expiration().touch(TouchMode.valueOf(value));
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

   protected void parseInvalidationCache(ConfigurationReader reader, ConfigurationBuilderHolder holder, String name, boolean template) {
      holder.pushScope(template ? ParserScope.CACHE_TEMPLATE : ParserScope.CACHE);
      if (!template && GlobUtils.isGlob(name))
         throw CONFIG.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      CacheMode baseCacheMode = configuration == null ? CacheMode.INVALIDATION_SYNC : CacheMode.INVALIDATION_SYNC.toSync(builder.clustering().cacheMode().isSynchronous());
      builder.clustering().cacheMode(baseCacheMode);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            default: {
               this.parseCacheElement(reader, element, holder);
            }
         }
      }
      holder.popScope();
   }

   private void parseSegmentedCacheAttribute(ConfigurationReader reader,
                                             int index, Attribute attribute, String value, ConfigurationBuilder builder, ClassLoader classLoader, CacheMode baseCacheMode)
      {
      switch (attribute) {
         case SEGMENTS: {
            builder.clustering().hash().numSegments(Integer.parseInt(value));
            break;
         }
         case CONSISTENT_HASH_FACTORY: {
            if (reader.getSchema().since(11, 0)) {
               CONFIG.debug("Consistent hash customization has been deprecated and will be removed");
            }
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

   private void parseClusteredCacheAttribute(ConfigurationReader reader,
         int index, Attribute attribute, String value, ConfigurationBuilder builder, CacheMode baseCacheMode)
         {
      switch (attribute) {
         case ASYNC_MARSHALLING: {
            if (reader.getSchema().since(9, 0)) {
               throw ParseUtils.attributeRemoved(reader, index);
            } else {
               CONFIG.ignoredReplicationQueueAttribute(attribute.getLocalName(), reader.getLocation().getLineNumber());
            }
            break;
         }
         case MODE: {
            Mode mode = Mode.valueOf(value);
            builder.clustering().cacheMode(mode.apply(baseCacheMode));
            break;
         }
         case QUEUE_SIZE:
         case QUEUE_FLUSH_INTERVAL: {
            if (reader.getSchema().since(11, 0)) {
               throw ParseUtils.attributeRemoved(reader, index);
            } else {
               CONFIG.ignoredReplicationQueueAttribute(attribute.getLocalName(), reader.getLocation().getLineNumber());
            }
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

   protected void parseReplicatedCache(ConfigurationReader reader, ConfigurationBuilderHolder holder, String name, boolean template) {
      holder.pushScope(template ? ParserScope.CACHE_TEMPLATE : ParserScope.CACHE);
      if (!template && GlobUtils.isGlob(name))
         throw CONFIG.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      CacheMode baseCacheMode = configuration == null ? CacheMode.REPL_SYNC : CacheMode.REPL_SYNC.toSync(builder.clustering().cacheMode().isSynchronous());
      builder.clustering().cacheMode(baseCacheMode);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         parseSegmentedCacheAttribute(reader, i, attribute, value, builder, holder.getClassLoader(), baseCacheMode);
      }

      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            default: {
               this.parseSharedStateCacheElement(reader, element, holder);
            }
         }
      }
      holder.popScope();
   }

   private void parseStateTransfer(ConfigurationReader reader, ConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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

   protected void parseDistributedCache(ConfigurationReader reader, ConfigurationBuilderHolder holder, String name, boolean template) {
      holder.pushScope(template ? ParserScope.CACHE_TEMPLATE : ParserScope.CACHE);
      if (!template && GlobUtils.isGlob(name))
         throw CONFIG.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      CacheMode baseCacheMode = configuration == null ? CacheMode.DIST_SYNC : CacheMode.DIST_SYNC.toSync(builder.clustering().cacheMode().isSynchronous());
      builder.clustering().cacheMode(baseCacheMode);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
            case CAPACITY:
               if (reader.getSchema().since(13, 0)) {
                  throw CONFIG.attributeRemoved(Attribute.CAPACITY.getLocalName());
               }
               CONFIG.configDeprecatedUseOther(Attribute.CAPACITY, Attribute.CAPACITY_FACTOR);
            case CAPACITY_FACTOR: {
               builder.clustering().hash().capacityFactor(Float.parseFloat(value));
               break;
            }
            default: {
               this.parseSegmentedCacheAttribute(reader, i, attribute, value, builder, holder.getClassLoader(), baseCacheMode);
            }
         }
      }

      while (reader.inTag()) {
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

   private void parseGroups(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      GroupsConfigurationBuilder groups = builder.clustering().hash().groups();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  groups.enabled();
               } else {
                  groups.disabled();
               }
               break;
            case GROUPER:
               // JSON/YAML
               for(String grouper : reader.getListAttributeValue(i)) {
                  groups.addGrouper(Util.getInstance(grouper, holder.getClassLoader()));
               }
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case GROUPER:
               if (reader.getAttributeCount() == 1) {
                  groups.addGrouper(Util.getInstance(ParseUtils.readStringAttributeElement(reader, "class"), holder.getClassLoader()));
               } else {
                  groups.addGrouper(Util.getInstance(reader.getElementText(), holder.getClassLoader()));
                  reader.nextElement();
               }
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   protected void parseScatteredCache(ConfigurationReader reader, ConfigurationBuilderHolder holder, String name, boolean template) {
      if (!template && GlobUtils.isGlob(name))
         throw CONFIG.wildcardsNotAllowedInCacheNames(name);
      String configuration = reader.getAttributeValue(Attribute.CONFIGURATION.getLocalName());
      ConfigurationBuilder builder = getConfigurationBuilder(holder, name, template, configuration);
      CacheMode baseCacheMode = configuration == null ? CacheMode.SCATTERED_SYNC : CacheMode.SCATTERED_SYNC.toSync(builder.clustering().cacheMode().isSynchronous());
      ClusteringConfigurationBuilder clusteringBuilder = builder.clustering();
      clusteringBuilder.cacheMode(baseCacheMode);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case INVALIDATION_BATCH_SIZE: {
               clusteringBuilder.invalidationBatchSize(Integer.parseInt(value));
               break;
            }
            case BIAS_ACQUISITION: {
               clusteringBuilder.biasAcquisition(BiasAcquisition.valueOf(value));
               break;
            }
            case BIAS_LIFESPAN: {
               clusteringBuilder.biasLifespan(Long.parseLong(value), TimeUnit.MILLISECONDS);
               break;
            }
            default: {
               this.parseSegmentedCacheAttribute(reader, i, attribute, value, builder, holder.getClassLoader(), baseCacheMode);
            }
         }
      }

      while (reader.inTag()) {
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
         throw CONFIG.duplicateCacheName(name);
      }
      ConfigurationBuilder builder = holder.newConfigurationBuilder(name);
      if (baseConfigurationName != null) {
         ConfigurationBuilder baseConfigurationBuilder = holder.getNamedConfigurationBuilders().get(baseConfigurationName);
         if (baseConfigurationBuilder == null) {
            throw CONFIG.undeclaredConfiguration(baseConfigurationName, name);
         }
         Configuration baseConfiguration = baseConfigurationBuilder.build();
         if (!baseConfiguration.isTemplate()) {
            throw CONFIG.noConfiguration(baseConfigurationName);
         }
         builder.read(baseConfiguration);
      }

      return builder.template(template);
   }

   private void parsePersistence(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLUSTER_LOADER:
               CONFIG.warnUsingDeprecatedClusterLoader();
               parseClusterLoader(reader, holder);
               break;
            case FILE_STORE:
               parseFileStore(reader, holder);
               break;
            case STORE:
               parseCustomStore(reader, holder);
               break;
            case LOADER:
               ignoreElement(reader, element);
               break;
            case SINGLE_FILE_STORE:
               CONFIG.warnUsingDeprecatedClusterLoader();
               parseSingleFileStore(reader, holder);
               break;
            default:
               reader.handleAny(holder);
         }
      }
   }

   private void parseClusterLoader(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      ClusterLoaderConfigurationBuilder cclb = builder.persistence().addClusterLoader();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         String attrName = reader.getAttributeName(i);
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

   protected void parseFileStore(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      SoftIndexFileStoreConfigurationBuilder fileStoreBuilder = null;

      PersistenceConfigurationBuilder persistence = holder.getCurrentConfigurationBuilder().persistence();
      AbstractStoreConfigurationBuilder<?, ?> actualStoreConfig;
      int majorSchema = reader.getSchema().getMajor();
      boolean legacyFileStore = false;
      if (majorSchema < 13) {
         parseSingleFileStore(reader, holder);
         return;
      } else if (majorSchema == 13) {
         fileStoreBuilder = persistence.addStore(SFSToSIFSConfigurationBuilder.class);
         actualStoreConfig = fileStoreBuilder;
         legacyFileStore = true;
      } else {
         fileStoreBuilder = persistence.addSoftIndexFileStore();
         actualStoreConfig = fileStoreBuilder;
      }
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case RELATIVE_TO: {
               if (reader.getSchema().since(11, 0))
                  throw ParseUtils.attributeRemoved(reader, i);
               ignoreAttribute(reader, i);
               break;
            }
            case PATH: {
               fileStoreBuilder.dataLocation(value);
               fileStoreBuilder.indexLocation(value);
               break;
            }
            case FRAGMENTATION_FACTOR:
            case MAX_ENTRIES: {
               if (legacyFileStore) {
                  ignoreAttribute(reader, i);
               } else {
                  throw ParseUtils.attributeRemoved(reader, i);
               }
               break;
            }
            case OPEN_FILES_LIMIT:
               if (fileStoreBuilder != null) {
                  fileStoreBuilder.openFilesLimit(Integer.parseInt(value));
               } else {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               }
               break;
            case COMPACTION_THRESHOLD:
               if (fileStoreBuilder != null) {
                  fileStoreBuilder.compactionThreshold(Double.parseDouble(value));
               } else {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               }
               break;
            case PURGE: {
               actualStoreConfig.purgeOnStartup(Boolean.parseBoolean(value));
               break;
            }
            default: {
               parseStoreAttribute(reader, i, actualStoreConfig);
            }
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case DATA:
               if (fileStoreBuilder != null) {
                  parseData(reader, fileStoreBuilder);
               } else {
                  throw ParseUtils.unexpectedElement(reader);
               }
               break;
            case INDEX:
               if (fileStoreBuilder != null) {
                  parseIndex(reader, fileStoreBuilder);
               } else {
                  throw ParseUtils.unexpectedElement(reader);
               }
               break;
            default:
               parseStoreElement(reader, actualStoreConfig);
         }
      }
   }

   private void parseData(ConfigurationReader reader, SoftIndexFileStoreConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case PATH: {
               builder.dataLocation(value);
               break;
            }
            case RELATIVE_TO: {
               if (reader.getSchema().since(13, 0))
                  throw ParseUtils.attributeRemoved(reader, i);
               ignoreAttribute(reader, i);
               break;
            }
            case MAX_FILE_SIZE:
               builder.maxFileSize(Integer.parseInt(value));
               break;
            case SYNC_WRITES:
               builder.syncWrites(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseIndex(ConfigurationReader reader, SoftIndexFileStoreConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case PATH: {
               builder.indexLocation(value);
               break;
            }
            case RELATIVE_TO: {
               if (reader.getSchema().since(13, 0))
                  throw ParseUtils.attributeRemoved(reader, i);
               ignoreAttribute(reader, i);
               break;
            }
            case SEGMENTS:
               builder.indexSegments(Integer.parseInt(value));
               break;
            case INDEX_QUEUE_LENGTH:
               builder.indexQueueLength(Integer.parseInt(value));
               break;
            case MIN_NODE_SIZE:
               builder.minNodeSize(Integer.parseInt(value));
               break;
            case MAX_NODE_SIZE:
               builder.maxNodeSize(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   protected void parseSingleFileStore(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      SingleFileStoreConfigurationBuilder storeBuilder = holder.getCurrentConfigurationBuilder().persistence().addSingleFileStore();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case RELATIVE_TO: {
               if (reader.getSchema().since(11, 0))
                  throw ParseUtils.attributeRemoved(reader, i);
               ignoreAttribute(reader, i);
               break;
            }
            case PATH: {
               storeBuilder.location(value);
               break;
            }
            case MAX_ENTRIES: {
               storeBuilder.maxEntries(Integer.parseInt(value));
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
      this.parseStoreElements(reader, storeBuilder);
   }

   /**
    * This method is public static so that it can be reused by custom cache store/loader configuration parsers
    */
   public static void parseStoreAttribute(ConfigurationReader reader, int index, AbstractStoreConfigurationBuilder<?, ?> storeBuilder) {
      // Stores before 10.0 were non segmented by default
      if (reader.getSchema().getMajor() < 10) {
         storeBuilder.segmented(false);
      }
      String value = reader.getAttributeValue(index);
      Attribute attribute = Attribute.forName(reader.getAttributeName(index));
      switch (attribute) {
         case SHARED: {
            storeBuilder.shared(Boolean.parseBoolean(value));
            break;
         }
         case READ_ONLY: {
            storeBuilder.ignoreModifications(Boolean.parseBoolean(value));
            break;
         }
         case PRELOAD: {
            storeBuilder.preload(Boolean.parseBoolean(value));
            break;
         }
         case FETCH_STATE: {
            if (reader.getSchema().since(14, 0)) {
               throw ParseUtils.attributeRemoved(reader, index);
            } else {
               ignoreAttribute(reader, index);
            }
            break;
         }
         case PURGE: {
            storeBuilder.purgeOnStartup(Boolean.parseBoolean(value));
            break;
         }
         case SINGLETON: {
            if (reader.getSchema().since(10, 0)) {
               throw ParseUtils.attributeRemoved(reader, index);
            } else {
               ignoreAttribute(reader, index);
            }
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

   private void parseStoreElements(ConfigurationReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) {
      while (reader.inTag()) {
         parseStoreElement(reader, storeBuilder);
      }
   }

   public static void parseStoreElement(ConfigurationReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) {
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
         case PROPERTIES: {
            parseStoreProperties(reader, storeBuilder);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   public static void parseStoreWriteBehind(ConfigurationReader reader, AsyncStoreConfigurationBuilder<?> storeBuilder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case FLUSH_LOCK_TIMEOUT:
            case SHUTDOWN_TIMEOUT: {
               if (reader.getSchema().since(9, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ignoreAttribute(reader, i);
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
            case THREAD_POOL_SIZE: {
               if (reader.getSchema().since(11, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ignoreAttribute(reader, i);
               }
               break;
            }
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   public static void parseStoreProperty(ConfigurationReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) {
      String property = ParseUtils.requireSingleAttribute(reader, Attribute.NAME.getLocalName());
      String value = reader.getElementText();
      storeBuilder.addProperty(property, value);
   }

   public static void parseStoreProperties(ConfigurationReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         storeBuilder.addProperty(reader.getAttributeName(i, NamingStrategy.IDENTITY), reader.getAttributeValue(i));
      }

      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         if (element != Element.PROPERTY) {
            throw ParseUtils.unexpectedElement(reader, element);
         }
         parseStoreProperty(reader, storeBuilder);
      }
   }

   private void parseCustomStore(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Boolean fetchPersistentState = null;
      Boolean ignoreModifications = null;
      Boolean purgeOnStartup = null;
      Boolean preload = null;
      Boolean shared = null;
      Boolean transactional = null;
      Boolean segmented = null;
      Object store = null;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
               if (reader.getSchema().since(10, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ignoreAttribute(reader, i);
               }
               break;
            case TRANSACTIONAL:
               transactional = Boolean.parseBoolean(value);
               break;
            case SEGMENTED:
               segmented = Boolean.parseBoolean(value);
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
            if (transactional != null)
               sfs.transactional(transactional);
            if (segmented != null)
               sfs.segmented(segmented);
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
            if (segmented != null)
               configBuilder.segmented(segmented);

            parseStoreElements(reader, configBuilder);
         }
      }
   }

   private void parseIndexing(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      boolean selfEnable = reader.getSchema().since(11, 0);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case ENABLED:
               if (reader.getSchema().since(11, 0)) {
                  builder.indexing().enabled(Boolean.parseBoolean(value));
                  selfEnable = false;
               } else {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               }
               break;
            case INDEX:
               if (reader.getSchema().since(11, 0)) {
                  CONFIG.indexModeDeprecated();
               }
               if ("LOCAL".equals(value) || "PRIMARY_OWNER".equals(value)) {
                  throw CONFIG.indexModeNotSupported(value);
               }
               Index index = Index.valueOf(value);
               builder.indexing().index(index);
               selfEnable = false;
               break;
            case AUTO_CONFIG:
               CONFIG.autoConfigDeprecated();
               builder.indexing().autoConfig(Boolean.parseBoolean(value));
               break;
            case STORAGE:
               builder.indexing().storage(IndexStorage.requireValid(value, CONFIG));
               break;
            case STARTUP_MODE:
               builder.indexing().startupMode(IndexStartupMode.requireValid(value, CONFIG));
               break;
            case PATH:
               builder.indexing().path(value);
               break;
            case INDEXED_ENTITIES:
               builder.indexing().addIndexedEntities(reader.getListAttributeValue(i));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      if (selfEnable) {
         // The presence of the <indexing> element without any explicit enabling or disabling results in auto-enabling indexing since 11.0
         builder.indexing().enable();
      }

      Properties indexingProperties = new Properties();
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case KEY_TRANSFORMERS: {
               parseKeyTransformers(reader, holder, builder);
               break;
            }
            case INDEXED_ENTITIES: {
               parseIndexedEntities(reader, holder, builder);
               break;
            }
            case PROPERTY: {
               if (reader.getSchema().since(12, 0)) {
                  CONFIG.deprecatedIndexProperties();
               }
               parseProperty(reader, indexingProperties);
               break;
            }
            case INDEX_READER: {
               parseIndexReader(reader, builder);
               break;
            }
            case INDEX_WRITER: {
               parseIndexWriter(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      if(!indexingProperties.isEmpty()) {
         builder.indexing().withProperties(indexingProperties);
      }
   }

   private void parseKeyTransformers(ConfigurationReader reader, ConfigurationBuilderHolder holder, ConfigurationBuilder builder) {
      ParseUtils.requireNoAttributes(reader);
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case KEY_TRANSFORMER: {
               parseKeyTransformer(reader, holder, builder);
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseKeyTransformer(ConfigurationReader reader, ConfigurationBuilderHolder holder, ConfigurationBuilder builder) {
      String[] attrs = ParseUtils.requireAttributes(reader, Attribute.KEY.getLocalName(), Attribute.TRANSFORMER.getLocalName());
      Class<?> keyClass = Util.loadClass(attrs[0], holder.getClassLoader());
      Class<?> transformerClass = Util.loadClass(attrs[1], holder.getClassLoader());
      builder.indexing().addKeyTransformer(keyClass, transformerClass);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case KEY:
            case TRANSFORMER:
               // Already handled
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseIndexReader(ConfigurationReader reader, ConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         if (attribute == Attribute.REFRESH_INTERVAL) {
            builder.indexing().reader().refreshInterval(Long.parseLong(value));
         } else {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseIndexWriter(ConfigurationReader reader, ConfigurationBuilder builder) {
      IndexWriterConfigurationBuilder indexWriterBuilder = builder.indexing().writer();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         String value = reader.getAttributeValue(i);
         switch (attribute) {
            case THREAD_POOL_SIZE:
               indexWriterBuilder.threadPoolSize(Integer.parseInt(value));
               break;
            case COMMIT_INTERVAL:
               indexWriterBuilder.commitInterval(Integer.parseInt(value));
               break;
            case QUEUE_COUNT:
               indexWriterBuilder.queueCount(Integer.parseInt(value));
               break;
            case QUEUE_SIZE:
               indexWriterBuilder.queueSize(Integer.parseInt(value));
               break;
            case LOW_LEVEL_TRACE:
               indexWriterBuilder.setLowLevelTrace(Boolean.parseBoolean(value));
               break;
            case MAX_BUFFERED_ENTRIES:
               indexWriterBuilder.maxBufferedEntries(Integer.parseInt(value));
               break;
            case RAM_BUFFER_SIZE:
               indexWriterBuilder.ramBufferSize(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         if (element == Element.INDEX_MERGE) {
            parseIndexWriterMerge(reader, builder);
         } else {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseIndexWriterMerge(ConfigurationReader reader, ConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         String value = reader.getAttributeValue(i);
         IndexMergeConfigurationBuilder mergeBuilder = builder.indexing().writer().merge();
         switch (attribute) {
            case MAX_ENTRIES:
               mergeBuilder.maxEntries(Integer.parseInt(value));
               break;
            case CALIBRATE_BY_DELETES:
               mergeBuilder.calibrateByDeletes(Boolean.parseBoolean(value));
               break;
            case FACTOR:
               mergeBuilder.factor(Integer.parseInt(value));
               break;
            case MAX_FORCED_SIZE:
               mergeBuilder.maxForcedSize(Integer.parseInt(value));
               break;
            case MAX_SIZE:
               mergeBuilder.maxSize(Integer.parseInt(value));
               break;
            case MIN_SIZE:
               mergeBuilder.minSize(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseIndexedEntities(ConfigurationReader reader, ConfigurationBuilderHolder holder, ConfigurationBuilder builder) {
      ParseUtils.requireNoAttributes(reader);
      boolean isProtobufStorage = builder.memory().encoding().value().isProtobufStorage();
      String[] entities = reader.readArray(Element.INDEXED_ENTITIES, Element.INDEXED_ENTITY);
      for(String entity : entities) {
         builder.indexing().addIndexedEntities(entity);
         // Do not attempt to resolve type names to Java Classes if the cache uses Protobuf storage
         if (!isProtobufStorage) {
            try {
               Class<?> indexedClass = Util.loadClass(entity, holder.getClassLoader());
               builder.indexing().addIndexedEntity(indexedClass);
            } catch (Exception e) {
               // ignore
            }
         }
      }
   }

   private static void parseProperty(ConfigurationReader reader, Properties properties) {
      int attributes = reader.getAttributeCount();
      ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName());
      String key = null;
      String propertyValue = null;
      for (int i = 0; i < attributes; i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME: {
               key = value;
               break;
            }
            case VALUE: {
               propertyValue = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      if (propertyValue == null) {
         propertyValue = reader.getElementText();
      }
      properties.setProperty(key, propertyValue);
   }

   public static Properties parseProperties(final ConfigurationReader reader, Enum<?> outerElement) {
      return parseProperties(reader, outerElement.toString(), Element.PROPERTIES.toString(), Element.PROPERTY.toString());
   }

   public static Properties parseProperties(final ConfigurationReader reader, Enum<?> outerElement, Enum<?> collectionElement, Enum<?> itemElement) {
      return parseProperties(reader, outerElement.toString(), collectionElement.toString(), itemElement.toString());
   }

   public static Properties parseProperties(final ConfigurationReader reader, String outerElement, String collectionElement, String itemElement) {
      Properties properties = new Properties();
      while (reader.hasNext()) {
         ConfigurationReader.ElementType type = reader.nextElement();
         String element = reader.getLocalName();
         if (element.equals(collectionElement)) {
            // JSON/YAML map properties to attributes
            for (int i = 0; i < reader.getAttributeCount(); i++) {
               properties.setProperty(reader.getAttributeName(i), reader.getAttributeValue(i));
            }
         } else if (element.equals(itemElement)) {
            if (type == ConfigurationReader.ElementType.START_ELEMENT) {
               parseProperty(reader, properties);
            }
         } else if (type == ConfigurationReader.ElementType.END_ELEMENT && reader.getLocalName().equals(outerElement)) {
            return properties;
         } else {
            throw ParseUtils.unexpectedElement(reader);
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

      public static TransactionMode fromConfiguration(TransactionConfiguration transactionConfiguration, boolean batchingEnabled) {
         org.infinispan.transaction.TransactionMode mode = transactionConfiguration.transactionMode();
         boolean recoveryEnabled = transactionConfiguration.recovery().enabled();
         boolean xaEnabled = !batchingEnabled && !transactionConfiguration.useSynchronization();

         if (mode == org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL) {
            return NONE;
         }
         for(TransactionMode txMode : TransactionMode.values()) {
            if (txMode.mode == mode && txMode.xaEnabled == xaEnabled && txMode.recoveryEnabled == recoveryEnabled && txMode.batchingEnabled == batchingEnabled)
               return txMode;
         }
         throw CONFIG.unknownTransactionConfiguration(mode, xaEnabled, recoveryEnabled, batchingEnabled);
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
