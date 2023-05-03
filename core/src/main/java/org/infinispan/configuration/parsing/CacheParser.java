package org.infinispan.configuration.parsing;

import static org.infinispan.configuration.parsing.ParseUtils.ignoreAttribute;
import static org.infinispan.configuration.parsing.ParseUtils.ignoreElement;
import static org.infinispan.configuration.parsing.Parser.NAMESPACE;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

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
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ContentTypeConfigurationBuilder;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.infinispan.configuration.cache.EncodingConfigurationBuilder;
import org.infinispan.configuration.cache.GroupsConfigurationBuilder;
import org.infinispan.configuration.cache.IndexMergeConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.IndexWriterConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingMode;
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
@Namespace(root = "local-cache-configuration")
@Namespace(root = "distributed-cache")
@Namespace(root = "distributed-cache-configuration")
@Namespace(root = "replicated-cache")
@Namespace(root = "replicated-cache-configuration")
@Namespace(uri = NAMESPACE + "*", root = "local-cache")
@Namespace(uri = NAMESPACE + "*", root = "local-cache-configuration")
@Namespace(uri = NAMESPACE + "*", root = "distributed-cache")
@Namespace(uri = NAMESPACE + "*", root = "distributed-cache-configuration")
@Namespace(uri = NAMESPACE + "*", root = "replicated-cache")
@Namespace(uri = NAMESPACE + "*", root = "replicated-cache-configuration")
public class CacheParser implements ConfigurationParser {
   public static final String NAMESPACE = "urn:infinispan:config:";
   public static final String IGNORE_MISSING_TEMPLATES = "org.infinispan.parser.ignoreMissingTemplates";

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
         default:
               throw ParseUtils.unexpectedElement(reader);
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
      ConfigurationBuilder builder = getConfigurationBuilder(reader, holder, name, template, configuration);
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
            builder.simpleCache(ParseUtils.parseBoolean(reader, index, value));
            break;
         case STATISTICS: {
            builder.statistics().enabled(ParseUtils.parseBoolean(reader, index, value));
            break;
         }
         case STATISTICS_AVAILABLE: {
            builder.statistics().available(ParseUtils.parseBoolean(reader, index, value));
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
            builder.unsafe().unreliableReturnValues(ParseUtils.parseBoolean(reader, index, value));
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
      ParseUtils.parseAttributes(reader, builder.sites());
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
               ph.whenSplit(ParseUtils.parseEnum(reader, i, PartitionHandling.class, value));
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
      ParseUtils.parseAttributes(reader, backup);

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
      ParseUtils.parseAttributes(reader, backup.takeOffline());
      ParseUtils.requireNoContent(reader);
   }

   private void parseXSiteStateTransfer(ConfigurationReader reader, BackupConfigurationBuilder backup) {
      ParseUtils.parseAttributes(reader, backup.stateTransfer());
      ParseUtils.requireNoContent(reader);
   }

   private void parseBackupFor(ConfigurationReader reader, ConfigurationBuilder builder) {
      builder.sites().backupFor().reset();
      ParseUtils.parseAttributes(reader, builder.sites().backupFor());
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
         case QUERY: {
            this.parseQuery(reader, holder);
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
         ParseUtils.parseAttributes(reader, memoryBuilder);
      }
      if (reader.getSchema().since(15, 0)) {
         ParseUtils.requireNoContent(reader);
      } else {
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
   }

   private void parseOffHeapMemoryAttributes(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case SIZE:
               memoryBuilder.size(ParseUtils.parseLong(reader, i, value));
               break;
            case EVICTION:
               memoryBuilder.evictionType(ParseUtils.parseEnum(reader, i, EvictionType.class, value));
               break;
            case ADDRESS_COUNT:
               if (reader.getSchema().since(10, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ignoreAttribute(reader, i);
               }
               break;
            case STRATEGY:
               memoryBuilder.evictionStrategy(ParseUtils.parseEnum(reader, i, EvictionStrategy.class, value));
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
               memoryBuilder.size(ParseUtils.parseLong(reader, i, value));
               break;
            case STRATEGY:
               memoryBuilder.evictionStrategy(ParseUtils.parseEnum(reader, i, EvictionStrategy.class, value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseBinaryMemoryAttributes(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      MemoryConfigurationBuilder memoryBuilder = holder.getCurrentConfigurationBuilder().memory();
      ParseUtils.parseAttributes(reader, memoryBuilder.legacyBuilder());
      ParseUtils.requireNoContent(reader);
   }

   private void parseStoreAsBinary(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      CONFIG.configDeprecatedUseOther(Element.STORE_AS_BINARY, Element.MEMORY, reader.getLocation());
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
               binaryKeys = ParseUtils.parseBoolean(reader, i, value);
               break;
            case STORE_VALUES_AS_BINARY:
               binaryValues = ParseUtils.parseBoolean(reader, i, value);
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
               if (ParseUtils.parseBoolean(reader, i, value)) {
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
               interceptorBuilder.index(ParseUtils.parseInt(reader, i, value));
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

   private void parseLocking(ConfigurationReader reader, ConfigurationBuilder builder) {
      ParseUtils.parseAttributes(reader, builder.locking());
      ParseUtils.requireNoContent(reader);
   }

   private void parseTransaction(ConfigurationReader reader, ConfigurationBuilder builder, ConfigurationBuilderHolder holder) {
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
               builder.transaction().cacheStopTimeout(ParseUtils.parseLong(reader, i, value));
               break;
            }
            case MODE: {
               TransactionMode txMode = ParseUtils.parseEnum(reader, i, TransactionMode.class, value);
               builder.transaction().transactionMode(txMode.getMode());
               builder.transaction().useSynchronization(!txMode.isXAEnabled() && txMode.getMode().isTransactional());
               builder.transaction().recovery().enabled(txMode.isRecoveryEnabled());
               builder.invocationBatching().enable(txMode.isBatchingEnabled());
               break;
            }
            case LOCKING: {
               builder.transaction().lockingMode(ParseUtils.parseEnum(reader, i, LockingMode.class, value));
               break;
            }
            case TRANSACTION_MANAGER_LOOKUP_CLASS: {
               builder.transaction().transactionManagerLookup(Util.getInstance(value, holder.getClassLoader()));
               break;
            }
            case REAPER_WAKE_UP_INTERVAL: {
               builder.transaction().reaperWakeUpInterval(ParseUtils.parseLong(reader, i, value));
               break;
            }
            case COMPLETED_TX_TIMEOUT: {
               builder.transaction().completedTxTimeout(ParseUtils.parseLong(reader, i, value));
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
               builder.transaction().autoCommit(ParseUtils.parseBoolean(reader, i, value));
               break;
            }
            case RECOVERY_INFO_CACHE_NAME: {
               builder.transaction().recovery().recoveryInfoCacheName(value);
               break;
            }
            case NOTIFICATIONS: {
               builder.transaction().notifications(ParseUtils.parseBoolean(reader, i, value));
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
      CONFIG.configDeprecatedUseOther(Element.EVICTION, Element.MEMORY, reader.getLocation());
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
               long size = ParseUtils.parseLong(reader, i, value);
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
               builder.expiration().maxIdle(ParseUtils.parseLong(reader, i, value));
               break;
            }
            case LIFESPAN: {
               builder.expiration().lifespan(ParseUtils.parseLong(reader, i, value));
               break;
            }
            case INTERVAL: {
               builder.expiration().wakeUpInterval(ParseUtils.parseLong(reader, i, value));
               break;
            }
            case TOUCH: {
               if (reader.getSchema().since(12, 1)) {
                  builder.expiration().touch(ParseUtils.parseEnum(reader, i, TouchMode.class, value));
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
      ConfigurationBuilder builder = getConfigurationBuilder(reader, holder, name, template, configuration);
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
            builder.clustering().hash().numSegments(ParseUtils.parseInt(reader, index, value));
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
            Mode mode = ParseUtils.parseEnum(reader, index, Mode.class, value);
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
            builder.clustering().remoteTimeout(ParseUtils.parseLong(reader, index, value));
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
      ConfigurationBuilder builder = getConfigurationBuilder(reader, holder, name, template, configuration);
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
               builder.clustering().stateTransfer().awaitInitialTransfer(ParseUtils.parseBoolean(reader, i, value));
               break;
            }
            case ENABLED: {
               builder.clustering().stateTransfer().fetchInMemoryState(ParseUtils.parseBoolean(reader, i, value));
               break;
            }
            case TIMEOUT: {
               builder.clustering().stateTransfer().timeout(ParseUtils.parseLong(reader, i, value));
               break;
            }
            case CHUNK_SIZE: {
               builder.clustering().stateTransfer().chunkSize(ParseUtils.parseInt(reader, i, value));
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
      ConfigurationBuilder builder = getConfigurationBuilder(reader, holder, name, template, configuration);
      CacheMode baseCacheMode = configuration == null ? CacheMode.DIST_SYNC : CacheMode.DIST_SYNC.toSync(builder.clustering().cacheMode().isSynchronous());
      builder.clustering().cacheMode(baseCacheMode);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case OWNERS: {
               builder.clustering().hash().numOwners(ParseUtils.parseInt(reader, i, value));
               break;
            }
            case L1_LIFESPAN: {
               long lifespan = ParseUtils.parseLong(reader, i, value);
               if (lifespan > 0)
                  builder.clustering().l1().enable().lifespan(lifespan);
               else
                  builder.clustering().l1().disable();
               break;
            }
            case INVALIDATION_CLEANUP_TASK_FREQUENCY: {
               builder.clustering().l1().cleanupTaskFrequency(ParseUtils.parseLong(reader, i, value));
               break;
            }
            case CAPACITY:
               if (reader.getSchema().since(13, 0)) {
                  throw CONFIG.attributeRemoved(Attribute.CAPACITY.getLocalName(), reader.getLocation());
               }
               CONFIG.configDeprecatedUseOther(Attribute.CAPACITY, Attribute.CAPACITY_FACTOR, reader.getLocation());
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
               if (ParseUtils.parseBoolean(reader, i, value)) {
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

   private ConfigurationBuilder getConfigurationBuilder(ConfigurationReader reader, ConfigurationBuilderHolder holder, String name, boolean template, String baseConfigurationName) {
      if (holder.getNamedConfigurationBuilders().containsKey(name)) {
         throw CONFIG.duplicateCacheName(name);
      }
      boolean ignoreMissingTemplates = reader.getProperty(IGNORE_MISSING_TEMPLATES) != null;
      ConfigurationBuilder builder = holder.newConfigurationBuilder(name);
      if (baseConfigurationName != null) {
         ConfigurationBuilder baseConfigurationBuilder = holder.getNamedConfigurationBuilders().get(baseConfigurationName);
         if (baseConfigurationBuilder == null) {
            if (ignoreMissingTemplates) {
               baseConfigurationBuilder = new ConfigurationBuilder().template(true);
            } else {
               throw CONFIG.undeclaredConfiguration(baseConfigurationName, name);
            }
         }
         Configuration baseConfiguration = baseConfigurationBuilder.build();
         if (!baseConfiguration.isTemplate()) {
            throw CONFIG.noConfiguration(baseConfigurationName);
         }
         builder.read(baseConfiguration);
         if (ignoreMissingTemplates) {
            builder.configuration(baseConfigurationName);
         }
      }

      return builder.template(template);
   }

   private void parsePersistence(final ConfigurationReader reader, final ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      ParseUtils.parseAttributes(reader, builder.persistence());
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
               cclb.remoteCallTimeout(ParseUtils.parseLong(reader, i, value));
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
                  fileStoreBuilder.openFilesLimit(ParseUtils.parseInt(reader, i, value));
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
               actualStoreConfig.purgeOnStartup(ParseUtils.parseBoolean(reader, i, value));
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
               builder.maxFileSize(ParseUtils.parseInt(reader, i, value));
               break;
            case SYNC_WRITES:
               builder.syncWrites(ParseUtils.parseBoolean(reader, i, value));
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
               builder.indexSegments(ParseUtils.parseInt(reader, i, value));
               break;
            case INDEX_QUEUE_LENGTH:
               builder.indexQueueLength(ParseUtils.parseInt(reader, i, value));
               break;
            case MIN_NODE_SIZE:
               builder.minNodeSize(ParseUtils.parseInt(reader, i, value));
               break;
            case MAX_NODE_SIZE:
               builder.maxNodeSize(ParseUtils.parseInt(reader, i, value));
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
               storeBuilder.maxEntries(ParseUtils.parseInt(reader, i, value));
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
            storeBuilder.shared(ParseUtils.parseBoolean(reader, index, value));
            break;
         }
         case READ_ONLY: {
            storeBuilder.ignoreModifications(ParseUtils.parseBoolean(reader, index, value));
            break;
         }
         case PRELOAD: {
            storeBuilder.preload(ParseUtils.parseBoolean(reader, index, value));
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
            storeBuilder.purgeOnStartup(ParseUtils.parseBoolean(reader, index, value));
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
            storeBuilder.transactional(ParseUtils.parseBoolean(reader, index, value));
            break;
         }
         case MAX_BATCH_SIZE: {
            storeBuilder.maxBatchSize(ParseUtils.parseInt(reader, index, value));
            break;
         }
         case SEGMENTED: {
            storeBuilder.segmented(ParseUtils.parseBoolean(reader, index, value));
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
               storeBuilder.modificationQueueSize(ParseUtils.parseInt(reader, i, value));
               break;
            }
            case FAIL_SILENTLY:
               storeBuilder.failSilently(ParseUtils.parseBoolean(reader, i, value));
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
               fetchPersistentState = ParseUtils.parseBoolean(reader, i, value);
               break;
            case READ_ONLY:
               ignoreModifications = ParseUtils.parseBoolean(reader, i, value);
               break;
            case PURGE:
               purgeOnStartup = ParseUtils.parseBoolean(reader, i, value);
               break;
            case PRELOAD:
               preload = ParseUtils.parseBoolean(reader, i, value);
               break;
            case SHARED:
               shared = ParseUtils.parseBoolean(reader, i, value);
               break;
            case SINGLETON:
               if (reader.getSchema().since(10, 0)) {
                  throw ParseUtils.attributeRemoved(reader, i);
               } else {
                  ignoreAttribute(reader, i);
               }
               break;
            case TRANSACTIONAL:
               transactional = ParseUtils.parseBoolean(reader, i, value);
               break;
            case SEGMENTED:
               segmented = ParseUtils.parseBoolean(reader, i, value);
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

   private void parseQuery(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case DEFAULT_MAX_RESULTS:
               builder.query().defaultMaxResults(ParseUtils.parseInt(reader, i, value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      // no nested properties at the moment
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
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
                  builder.indexing().enabled(ParseUtils.parseBoolean(reader, i, value));
                  selfEnable = false;
               } else {
                  throw ParseUtils.unexpectedAttribute(reader, i);
               }
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
            case INDEXING_MODE:
               builder.indexing().indexingMode(IndexingMode.requireValid(value));
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
            case INDEX_SHARDING: {
               parseIndexSharding(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
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
      ParseUtils.parseAttributes(reader, builder.indexing().reader());
      ParseUtils.requireNoContent(reader);
   }

   private void parseIndexWriter(ConfigurationReader reader, ConfigurationBuilder builder) {
      IndexWriterConfigurationBuilder indexWriterBuilder = builder.indexing().writer();
      ParseUtils.parseAttributes(reader, indexWriterBuilder);
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         if (element == Element.INDEX_MERGE) {
            parseIndexWriterMerge(reader, builder);
         } else {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseIndexSharding(ConfigurationReader reader, ConfigurationBuilder builder) {
      ParseUtils.parseAttributes(reader, builder.indexing().sharding());
      ParseUtils.requireNoContent(reader);
   }

   private void parseIndexWriterMerge(ConfigurationReader reader, ConfigurationBuilder builder) {
      IndexMergeConfigurationBuilder mergeBuilder = builder.indexing().writer().merge();
      ParseUtils.parseAttributes(reader, mergeBuilder);
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
