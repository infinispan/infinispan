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
package org.infinispan.configuration.parsing;

import static org.infinispan.configuration.cache.CacheMode.DIST_ASYNC;
import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.INVALIDATION_ASYNC;
import static org.infinispan.configuration.cache.CacheMode.INVALIDATION_SYNC;
import static org.infinispan.configuration.cache.CacheMode.LOCAL;
import static org.infinispan.configuration.cache.CacheMode.REPL_ASYNC;
import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;

import java.util.Properties;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.hash.Hash;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.*;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder.FsyncMode;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.cluster.ClusterCacheLoader;
import org.infinispan.loaders.file.FileCacheStore;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.staxmapper.XMLExtendedStreamReader;

import static org.infinispan.util.StringPropertyReplacer.replaceProperties;

/**
 * This class implements the parser for 5.2 schema files
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class Parser52 implements ConfigurationParser<ConfigurationBuilderHolder> {

   private static final Log log = LogFactory.getLog(Parser52.class);

   private static final Namespace NAMESPACES[] = {
      new Namespace(Namespace.INFINISPAN_NS_BASE_URI, Element.ROOT.getLocalName(), 5, 2),
   };

   public Parser52() {}

   @Override
   public Namespace[] getSupportedNamespaces() {
      return NAMESPACES;
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case DEFAULT: {
               parseDefaultCache(reader, holder);
               break;
            }
            case GLOBAL: {
               parseGlobal(reader, holder);
               break;
            }
            case NAMED_CACHE: {
               parseNamedCache(reader, holder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseNamedCache(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {

      ParseUtils.requireSingleAttribute(reader, Attribute.NAME.getLocalName());

      String name = "";

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               name = value;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      // Reuse the builder if it was made before
      ConfigurationBuilder builder = holder.getNamedConfigurationBuilders().get(name);
      if (builder == null) {
          builder = holder.newConfigurationBuilder(name);
      }
      parseCache(reader, holder);

   }

   private void parseDefaultCache(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      parseCache(reader, holder);
   }

   private void parseCache(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLUSTERING:
               parseClustering(reader, holder);
               break;
            case CUSTOM_INTERCEPTORS:
               parseCustomInterceptors(reader, holder);
               break;
            case DATA_CONTAINER:
               parseDataContainer(reader, holder);
               break;
            case DEADLOCK_DETECTION:
               parseDeadlockDetection(reader, holder);
               break;
            case EVICTION:
               parseEviction(reader, holder);
               break;
            case EXPIRATION:
               parseExpiration(reader, holder);
               break;
            case INDEXING:
               parseIndexing(reader, holder);
               break;
            case INVOCATION_BATCHING:
               parseInvocationBatching(reader, holder);
               break;
            case JMX_STATISTICS:
               parseJmxStatistics(reader, holder);
               break;
            case LOADERS:
               parseLoaders(reader, holder);
               break;
            case LOCKING:
               parseLocking(reader, holder);
               break;
            case MODULES:
               parseModules(reader, holder);
               break;
            case LAZY_DESERIALIZATION:
            case STORE_AS_BINARY:
               parseStoreAsBinary(reader, holder);
               break;
            case TRANSACTION:
               parseTransaction(reader, holder);
               break;
            case UNSAFE:
               parseUnsafe(reader, holder);
               break;
            case VERSIONING:
               parseVersioning(reader, holder);
               break;
            case SITES:
               parseLocalSites(reader, holder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseModules(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         reader.handleAny(holder);
      }
   }

   private void parseVersioning(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      builder.versioning().disable(); // Disabled by default.
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case VERSIONING_SCHEME:
               builder.versioning().scheme(VersioningScheme.valueOf(value));
               break;
            case ENABLED:
               builder.versioning().enable();
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseGlobalSites(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalConfigurationBuilder gcb = holder.getGlobalConfigurationBuilder();
      ParseUtils.requireSingleAttribute(reader, "local");
      String value = replaceProperties(reader.getAttributeValue(0));
      gcb.site().localSite(value);
      ParseUtils.requireNoContent(reader);
   }

   private void parseLocalSites(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder ccb = holder.getCurrentConfigurationBuilder();
      ParseUtils.requireNoAttributes(reader);
      boolean isEmptyTag = false;

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         isEmptyTag = true;
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case BACKUPS:
               //if backups is present then remove any existing backups as they were added
               // by the default config.
               ccb.sites().backups().clear();
               parseBackups(reader, ccb);
               break;
            case BACKUP_FOR:
               parseBackupFor(reader, ccb);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      if (!isEmptyTag) {
         ccb.sites().backups().clear();
         ccb.sites().backupFor().reset();
      }

   }

   private void parseBackupFor(XMLExtendedStreamReader reader, ConfigurationBuilder ccb) throws XMLStreamException {
      ccb.sites().backupFor().reset();
      BackupForBuilder backupForBuilder = ccb.sites().backupFor();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case REMOTE_SITE:
               backupForBuilder.remoteSite(value);
               break;
            case REMOTE_CACHE:
               backupForBuilder.remoteCache(value);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseBackups(XMLExtendedStreamReader reader, ConfigurationBuilder ccb) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case BACKUP: {
               parseBackup(reader, ccb);
            }
            default: {
               ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseBackup(XMLExtendedStreamReader reader, ConfigurationBuilder ccb) throws XMLStreamException {
      BackupConfigurationBuilder backup = ccb.sites().addBackup();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case TIMEOUT:
               backup.replicationTimeout(Long.parseLong(value));
               break;
            case STRATEGY:
               backup.strategy(BackupConfiguration.BackupStrategy.valueOf(value));
               break;
            case SITE:
               backup.site(value);
               break;
            case BACKUP_FAILURE_POLICY:
               backup.backupFailurePolicy(BackupFailurePolicy.valueOf(value));
               break;
            case USE_TWO_PHASE_COMMIT:
               backup.useTwoPhaseCommit(Boolean.parseBoolean(value));
               break;
            case FAILURE_POLICY_CLASS:
               backup.failurePolicyClass(value);
               break;
            case ENABLED:
               backup.enabled(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      parseTakeOffline(reader, backup);
   }

   private void parseTakeOffline(XMLExtendedStreamReader reader, BackupConfigurationBuilder backup) throws XMLStreamException {
      int count = 0;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         count++;
         Element takeOffline = Element.forName(reader.getLocalName());
         for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = replaceProperties(reader.getAttributeValue(i));
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
               case AFTER_FAILURES:
                  backup.takeOffline().afterFailures(Integer.parseInt(value));
                  break;
               case MIN_TIME_TO_WAIT:
                  backup.takeOffline().minTimeToWait(Long.parseLong(value));
                  break;
               default:
                  throw ParseUtils.unexpectedElement(reader);
            }
         }
         ParseUtils.requireNoContent(reader);
      }
      if (count > 1)
         throw new ConfigurationException("Only one 'takeOffline' element allowed within a 'backup'");
   }

   private void parseTransaction(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      boolean forceSetTransactional = false;
      boolean transactionModeSpecified = false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case AUTO_COMMIT:
               builder.transaction().autoCommit(Boolean.parseBoolean(value));
               break;
            case CACHE_STOP_TIMEOUT:
               builder.transaction().cacheStopTimeout(Long.parseLong(value));
               break;
            case EAGER_LOCK_SINGLE_NODE:
               builder.transaction().eagerLockingSingleNode(Boolean.parseBoolean(value));
               break;
            case LOCKING_MODE:
               builder.transaction().lockingMode(LockingMode.valueOf(value));
               break;
            case SYNC_COMMIT_PHASE:
               builder.transaction().syncCommitPhase(Boolean.parseBoolean(value));
               break;
            case SYNC_ROLLBACK_PHASE:
               builder.transaction().syncRollbackPhase(Boolean.parseBoolean(value));
               break;
            case TRANSACTION_MANAGER_LOOKUP_CLASS:
               builder.transaction().transactionManagerLookup(Util.<TransactionManagerLookup>getInstance(value, holder.getClassLoader()));
               forceSetTransactional = true;
               break;
            case TRANSACTION_MODE:
               builder.transaction().transactionMode(TransactionMode.valueOf(value));
               transactionModeSpecified = true;
               break;
            case USE_EAGER_LOCKING:
               builder.transaction().useEagerLocking(Boolean.parseBoolean(value));
               break;
            case USE_SYNCHRONIZAION:
               builder.transaction().useSynchronization(Boolean.parseBoolean(value));
               break;
            case USE_1PC_FOR_AUTOCOMMIT_TX:
               builder.transaction().use1PcForAutoCommitTransactions(Boolean.parseBoolean(value));
               break;
            case REAPER_WAKE_UP_INTERVAL:
               builder.transaction().reaperWakeUpInterval(Long.parseLong(value));
               break;
            case COMPLETED_TX_TIMEOUT:
               builder.transaction().completedTxTimeout(Long.parseLong(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      if (!transactionModeSpecified && forceSetTransactional) {
         builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case RECOVERY:
               parseRecovery(reader, holder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }

   }

   private void parseRecovery(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      RecoveryConfigurationBuilder recovery = holder.getCurrentConfigurationBuilder().transaction().recovery();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  recovery.enable();
               } else {
                  recovery.disable();
               }
               break;
            case RECOVERY_INFO_CACHE_NAME:
               recovery.recoveryInfoCacheName(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseUnsafe(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case UNRELIABLE_RETURN_VALUES:
               builder.unsafe().unreliableReturnValues(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseStoreAsBinary(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  builder.storeAsBinary().enable();
               } else {
                  builder.storeAsBinary().disable();
               }
               break;
            case STORE_KEYS_AS_BINARY:
               builder.storeAsBinary().storeKeysAsBinary(Boolean.parseBoolean(value));
               break;
            case STORE_VALUES_AS_BINARY:
               builder.storeAsBinary().storeValuesAsBinary(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseLocking(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CONCURRENCY_LEVEL:
               builder.locking().concurrencyLevel(Integer.parseInt(value));
               break;
            case ISOLATION_LEVEL:
               builder.locking().isolationLevel(IsolationLevel.valueOf(value));
               break;
            case LOCK_ACQUISITION_TIMEOUT:
               builder.locking().lockAcquisitionTimeout(Long.parseLong(value));
               break;
            case USE_LOCK_STRIPING:
               builder.locking().useLockStriping(Boolean.parseBoolean(value));
               break;
            case WRITE_SKEW_CHECK:
               builder.locking().writeSkewCheck(Boolean.parseBoolean(value));
               break;
            case SUPPORTS_CONCURRENT_UPDATES:
               builder.locking().supportsConcurrentUpdates(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseLoaders(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PASSIVATION:
               builder.loaders().passivation(Boolean.parseBoolean(value));
               break;
            case PRELOAD:
               builder.loaders().preload(Boolean.parseBoolean(value));
               break;
            case SHARED:
               builder.loaders().shared(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLUSTER_LOADER:
               parseClusterLoader(reader, holder);
               break;
            case FILE_STORE:
               parseFileStore(reader, holder);
               break;
            case LOADER:
               parseLoader(reader, holder);
               break;
            case STORE:
               parseStore(reader, holder);
               break;
            default:
               reader.handleAny(holder);
         }
      }
   }

   private void parseClusterLoader(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      ClusterCacheLoaderConfigurationBuilder cclb = builder.loaders().addClusterCacheLoader();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case REMOTE_CALL_TIMEOUT:
            cclb.remoteCallTimeout(Long.parseLong(value));
            break;
         default:
            parseCommonLoaderAttributes(reader, i, cclb);
            break;
         }
      }
      parseLoaderChildren(reader, cclb);
   }

   private void parseFileStore(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      FileCacheStoreConfigurationBuilder fcscb = builder.loaders().addFileCacheStore();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case LOCATION:
            fcscb.location(value);
            break;
         case FSYNC_INTERVAL:
            fcscb.fsyncInterval(Long.parseLong(value));
            break;
         case FSYNC_MODE:
            fcscb.fsyncMode(FsyncMode.valueOf(value));
            break;
         case STREAM_BUFFER_SIZE:
            fcscb.streamBufferSize(Integer.parseInt(value));
            break;
         default:
            parseLockSupportStoreAttributes(reader, i, fcscb);
            break;
         }
      }
      parseStoreChildren(reader, fcscb);
   }

   /**
    * This method is public static so that it can be reused by custom cache store/loader configuration parsers
    */
   public static void parseLockSupportStoreAttributes(XMLExtendedStreamReader reader, int i,
         LockSupportStoreConfigurationBuilder<?, ?> builder) throws XMLStreamException {
      ParseUtils.requireNoNamespaceAttribute(reader, i);
      String value = replaceProperties(reader.getAttributeValue(i));
      Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
      switch (attribute) {
      case LOCK_ACQUISITION_TIMEOUT: {
         builder.lockAcquistionTimeout(Long.parseLong(value));
         break;
      }
      case CONCURRENCY_LEVEL: {
         builder.lockConcurrencyLevel(Integer.parseInt(value));
         break;
      }
      default: {
         parseCommonStoreAttributes(reader, i, builder);
      }
      }
   }

   /**
    * This method is public static so that it can be reused by custom cache store/loader configuration parsers
    */
   public static void parseCommonLoaderAttributes(XMLExtendedStreamReader reader, int i,
         CacheLoaderConfigurationBuilder<?, ?> builder) throws XMLStreamException {
      throw ParseUtils.unexpectedAttribute(reader, i);
   }

   /**
    * This method is public static so that it can be reused by custom cache store/loader configuration parsers
    */
   public static void parseCommonStoreAttributes(XMLExtendedStreamReader reader, int i,
         CacheStoreConfigurationBuilder<?, ?> builder) throws XMLStreamException {
      ParseUtils.requireNoNamespaceAttribute(reader, i);
      String value = replaceProperties(reader.getAttributeValue(i));
      Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
      switch (attribute) {
      case FETCH_PERSISTENT_STATE:
         builder.fetchPersistentState(Boolean.parseBoolean(value));
         break;
      case IGNORE_MODIFICATIONS:
         builder.ignoreModifications(Boolean.parseBoolean(value));
         break;
      case PURGE_ON_STARTUP:
         builder.purgeOnStartup(Boolean.parseBoolean(value));
         break;
      case PURGE_SYNCHRONOUSLY:
         builder.purgeSynchronously(Boolean.parseBoolean(value));
         break;
      case PURGER_THREADS:
         builder.purgerThreads(Integer.parseInt(value));
         break;
      default:
         throw ParseUtils.unexpectedAttribute(reader, i);
      }
   }

   private void parseLoader(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
     ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      CacheLoader loader = null;
      Boolean fetchPersistentState = null;
      Boolean ignoreModifications = null;
      Boolean purgeOnStartup = null;
      Integer purgerThreads = null;
      Boolean purgeSynchronously = null;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS:
               loader = Util.getInstance(value, holder.getClassLoader());
               break;
            /* The following attributes should be considered deprecated and removed in 6.0 */
            case FETCH_PERSISTENT_STATE:
               fetchPersistentState = Boolean.valueOf(value);
               break;
            case IGNORE_MODIFICATIONS:
               ignoreModifications = Boolean.valueOf(value);
               break;
            case PURGE_ON_STARTUP:
               purgeOnStartup = Boolean.valueOf(value);
               break;
            case PURGER_THREADS:
               purgerThreads = Integer.valueOf(value);
               break;
            case PURGE_SYNCHRONOUSLY:
               purgeSynchronously = Boolean.valueOf(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      if (loader != null) {
         if (loader instanceof FileCacheStore) {
            FileCacheStoreConfigurationBuilder fcscb = builder.loaders().addFileCacheStore();
            if (fetchPersistentState != null)
               fcscb.fetchPersistentState(fetchPersistentState);
            if (ignoreModifications != null)
               fcscb.ignoreModifications(ignoreModifications);
            if (purgeOnStartup != null)
               fcscb.purgeOnStartup(purgeOnStartup);
            if (purgeSynchronously != null)
               fcscb.purgeSynchronously(purgeSynchronously);
            parseStoreChildren(reader, fcscb);
         } else if (loader instanceof CacheStore){
            log.deprecatedLoaderAsStoreConfiguration();
            LegacyStoreConfigurationBuilder scb = builder.loaders().addStore();
            scb.cacheStore((CacheStore)loader);
            if (fetchPersistentState != null)
               scb.fetchPersistentState(fetchPersistentState);
            if (ignoreModifications != null)
               scb.ignoreModifications(ignoreModifications);
            if (purgerThreads != null)
               scb.purgerThreads(purgerThreads);
            if (purgeOnStartup != null)
               scb.purgeOnStartup(purgeOnStartup);
            if (purgeSynchronously != null)
               scb.purgeSynchronously(purgeSynchronously);
            parseStoreChildren(reader, scb);
         } else if (loader instanceof ClusterCacheLoader) {
            ClusterCacheLoaderConfigurationBuilder cclb = builder.loaders().addClusterCacheLoader();
            parseLoaderChildren(reader, cclb);
         } else {
            LegacyLoaderConfigurationBuilder lcb = builder.loaders().addLoader();
            lcb.cacheLoader(loader);
            parseLoaderChildren(reader, lcb);
         }
      }

   }

   private void parseStore(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      CacheStore store = null;
      Boolean fetchPersistentState = null;
      Boolean ignoreModifications = null;
      Boolean purgeOnStartup = null;
      Integer purgerThreads = null;
      Boolean purgeSynchronously = null;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS:
               store = Util.getInstance(value, holder.getClassLoader());
               break;
            case FETCH_PERSISTENT_STATE:
               fetchPersistentState = Boolean.valueOf(value);
               break;
            case IGNORE_MODIFICATIONS:
               ignoreModifications = Boolean.valueOf(value);
               break;
            case PURGE_ON_STARTUP:
               purgeOnStartup = Boolean.valueOf(value);
               break;
            case PURGER_THREADS:
               purgerThreads = Integer.valueOf(value);
               break;
            case PURGE_SYNCHRONOUSLY:
               purgeSynchronously = Boolean.valueOf(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      if (store != null) {
         if (store instanceof FileCacheStore) {
            FileCacheStoreConfigurationBuilder fcscb = builder.loaders().addFileCacheStore();
            if (fetchPersistentState != null)
               fcscb.fetchPersistentState(fetchPersistentState);
            if (ignoreModifications != null)
               fcscb.ignoreModifications(ignoreModifications);
            if (purgeOnStartup != null)
               fcscb.purgeOnStartup(purgeOnStartup);
            if (purgeSynchronously != null)
               fcscb.purgeSynchronously(purgeSynchronously);
            parseStoreChildren(reader, fcscb);
         } else {
            LegacyStoreConfigurationBuilder scb = builder.loaders().addStore();
            scb.cacheStore(store);
            if (fetchPersistentState != null)
               scb.fetchPersistentState(fetchPersistentState);
            if (ignoreModifications != null)
               scb.ignoreModifications(ignoreModifications);
            if (purgerThreads != null)
               scb.purgerThreads(purgerThreads);
            if (purgeOnStartup != null)
               scb.purgeOnStartup(purgeOnStartup);
            if (purgeSynchronously != null)
               scb.purgeSynchronously(purgeSynchronously);
            parseStoreChildren(reader, scb);
         }
      }
   }

   private void parseLoaderChildren(final XMLExtendedStreamReader reader, final CacheLoaderConfigurationBuilder<?, ?> loaderBuilder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         parseCommonLoaderChildren(reader, loaderBuilder);
      }
   }

   public static void parseCommonLoaderChildren(final XMLExtendedStreamReader reader,
         final CacheLoaderConfigurationBuilder<?, ?> loaderBuilder) throws XMLStreamException {
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case PROPERTIES:
            loaderBuilder.withProperties(parseProperties(reader));
            break;
         default:
            throw ParseUtils.unexpectedElement(reader);
      }
   }

   private void parseStoreChildren(final XMLExtendedStreamReader reader, final CacheStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         parseCommonStoreChildren(reader, storeBuilder);
      }
   }

   public static void parseCommonStoreChildren(final XMLExtendedStreamReader reader,
         final CacheStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case ASYNC:
            parseAsyncStore(reader, storeBuilder);
            break;
         case PROPERTIES:
            storeBuilder.withProperties(parseProperties(reader));
            break;
         case SINGLETON_STORE:
            parseSingletonStore(reader, storeBuilder);
            break;
         default:
            throw ParseUtils.unexpectedElement(reader);
      }
   }

   public static void parseSingletonStore(final XMLExtendedStreamReader reader, final CacheStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  storeBuilder.singletonStore().enable();
               } else {
                  storeBuilder.singletonStore().disable();
               }
               break;
            case PUSH_STATE_TIMEOUT:
               storeBuilder.singletonStore().pushStateTimeout(Long.parseLong(value));
               break;
            case PUSH_STATE_WHEN_COORDINATOR:
               storeBuilder.singletonStore().pushStateWhenCoordinator(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   public static void parseAsyncStore(final XMLExtendedStreamReader reader, final CacheStoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  storeBuilder.async().enable();
               } else {
                  storeBuilder.async().disable();
               }
               break;
            case FLUSH_LOCK_TIMEOUT:
               storeBuilder.async().flushLockTimeout(Long.parseLong(value));
               break;
            case MODIFICATION_QUEUE_SIZE:
               storeBuilder.async().modificationQueueSize(Integer.parseInt(value));
               break;
            case SHUTDOWN_TIMEOUT:
               storeBuilder.async().shutdownTimeout(Long.parseLong(value));
               break;
            case THREAD_POOL_SIZE:
               storeBuilder.async().threadPoolSize(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseJmxStatistics(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  builder.jmxStatistics().enable();
               } else {
                  builder.jmxStatistics().disable();
               }
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseInvocationBatching(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  builder.invocationBatching().enable();
               } else {
                  builder.invocationBatching().disable();
               }
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseIndexing(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  builder.indexing().enable();
               } else {
                  builder.indexing().disable();
               }
               break;
            case INDEX_LOCAL_ONLY:
                  builder.indexing().indexLocalOnly(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      Properties indexingProperties = null;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PROPERTIES: {
               indexingProperties = parseProperties(reader);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      IndexingConfigurationBuilder indexing = builder.indexing();
      if (indexingProperties != null) {
         indexing.withProperties(indexingProperties);
      }
   }

   private void parseExpiration(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case LIFESPAN:
               builder.expiration().lifespan(Long.parseLong(value));
               break;
            case MAX_IDLE:
               builder.expiration().maxIdle(Long.parseLong(value));
               break;
            case REAPER_ENABLED:
               if (Boolean.parseBoolean(value)) {
                  builder.expiration().enableReaper();
               } else {
                  builder.expiration().disableReaper();
               }
               break;
            case WAKE_UP_INTERVAL:
               builder.expiration().wakeUpInterval(Long.parseLong(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseEviction(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case MAX_ENTRIES:
               builder.eviction().maxEntries(Integer.parseInt(value));
               break;
            case STRATEGY:
               builder.eviction().strategy(EvictionStrategy.valueOf(value));
               break;
            case THREAD_POLICY:
               builder.eviction().threadPolicy(EvictionThreadPolicy.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseDeadlockDetection(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  builder.deadlockDetection().enable();
               } else {
                  builder.deadlockDetection().disable();
               }
               break;
            case SPIN_DURATION:
               builder.deadlockDetection().spinDuration(Long.parseLong(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseDataContainer(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS:
               builder.dataContainer().dataContainer(Util.<DataContainer>getInstance(value, holder.getClassLoader()));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PROPERTIES:
               builder.dataContainer().withProperties(parseProperties(reader));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseCustomInterceptors(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case INTERCEPTOR:
               parseInterceptor(reader, holder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }

   }

   private void parseInterceptor(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case AFTER:
               interceptorBuilder.after(Util.<CommandInterceptor>loadClass(value, holder.getClassLoader()));
               break;
            case BEFORE:
               interceptorBuilder.before(Util.<CommandInterceptor>loadClass(value, holder.getClassLoader()));
               break;
            case CLASS:
               interceptorBuilder.interceptor(Util.<CommandInterceptor>getInstance(value, holder.getClassLoader()));
               break;
            case INDEX:
               interceptorBuilder.index(Integer.parseInt(value));
               break;
            case POSITION:
               interceptorBuilder.position(Position.valueOf(value.toUpperCase()));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PROPERTIES: {
               interceptorBuilder.withProperties(parseProperties(reader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseClustering(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {

      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      String clusteringMode = null;
      boolean synchronous = false;
      boolean asynchronous = false;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case MODE:
               clusteringMode = value;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ASYNC:
               asynchronous = true;
               setMode(builder, clusteringMode, asynchronous, synchronous, reader);
               parseAsync(reader, holder);
               break;
            case HASH:
               parseHash(reader, holder);
               break;
            case L1:
               parseL1reader(reader, holder.getCurrentConfigurationBuilder());
               break;
            case STATE_TRANSFER:
               parseStateTransfer(reader, holder.getCurrentConfigurationBuilder());
               break;
            case SYNC:
               synchronous = true;
               setMode(builder, clusteringMode, asynchronous, synchronous, reader);
               parseSync(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }

      if (!synchronous && !asynchronous) {
         setMode(builder, clusteringMode, asynchronous, asynchronous, reader);
      }


   }

   private void setMode(final ConfigurationBuilder builder, final String clusteringMode, final boolean asynchronous, final boolean synchronous, final XMLExtendedStreamReader reader) {
      if (synchronous && asynchronous) {
         throw new ConfigurationException("Cannot configure <sync> and <async> on the same cluster, " + reader.getLocation());
      }

      if (clusteringMode != null) {
         String mode = clusteringMode.toUpperCase();
         if (ParsedCacheMode.REPL.matches(mode)) {
            if (!asynchronous) {
               builder.clustering().cacheMode(REPL_SYNC);
            } else {
               builder.clustering().cacheMode(REPL_ASYNC);
            }
         } else if (ParsedCacheMode.INVALIDATION.matches(mode)) {
            if (!asynchronous) {
               builder.clustering().cacheMode(INVALIDATION_SYNC);
            } else {
               builder.clustering().cacheMode(INVALIDATION_ASYNC);
            }
         } else if (ParsedCacheMode.DIST.matches(mode)) {
            if (!asynchronous) {
               builder.clustering().cacheMode(DIST_SYNC);
            } else {
               builder.clustering().cacheMode(DIST_ASYNC);
            }
         } else if (ParsedCacheMode.LOCAL.matches(mode)) {
            builder.clustering().cacheMode(LOCAL);
         } else {
            throw new ConfigurationException("Invalid clustering mode " + clusteringMode + ", " + reader.getLocation());
         }
      } else {
         // If no cache mode is given but sync or async is specified, default to DIST
         if (synchronous) {
            builder.clustering().cacheMode(DIST_SYNC);
         } else if (asynchronous) {
            builder.clustering().cacheMode(DIST_ASYNC);
         }
      }
   }

   private void parseSync(final XMLExtendedStreamReader reader, final ConfigurationBuilder builder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case REPL_TIMEOUT:
               builder.clustering().sync().replTimeout(Long.parseLong(value));
               break;

            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseStateTransfer(final XMLExtendedStreamReader reader, final ConfigurationBuilder builder) throws XMLStreamException{

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FETCH_IN_MEMORY_STATE:
               builder.clustering().stateTransfer().fetchInMemoryState(Boolean.parseBoolean(value));
               break;
            case AWAIT_INITIAL_TRANSFER:
               builder.clustering().stateTransfer().awaitInitialTransfer(Boolean.parseBoolean(value));
               break;
            case TIMEOUT:
               builder.clustering().stateTransfer().timeout(Long.parseLong(value));
               break;
            case CHUNK_SIZE:
               builder.clustering().stateTransfer().chunkSize(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseL1reader(final XMLExtendedStreamReader reader, final ConfigurationBuilder builder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  builder.clustering().l1().enable();
               } else {
                  builder.clustering().l1().disable();
               }
               break;
            case INVALIDATION_THRESHOLD:
               builder.clustering().l1().invalidationThreshold(Integer.parseInt(value));
               break;
            case LIFESPAN:
               builder.clustering().l1().lifespan(Long.parseLong(value));
               break;
            case INVALIDATION_CLEANUP_TASK_FREQUENCY:
               builder.clustering().l1().cleanupTaskFrequency(Long.parseLong(value));
               break;
            case ON_REHASH:
               if (Boolean.parseBoolean(value)) {
                  builder.clustering().l1().enableOnRehash();
               } else {
                  builder.clustering().l1().disableOnRehash();
               }
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseHash(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FACTORY:
               builder.clustering().hash().consistentHashFactory(Util.<ConsistentHashFactory>getInstance(value, holder.getClassLoader()));
               break;
            case HASH_FUNCTION_CLASS:
               builder.clustering().hash().hash(Util.<Hash>getInstance(value, holder.getClassLoader()));
               break;
            case NUM_OWNERS:
               builder.clustering().hash().numOwners(Integer.parseInt(value));
               break;
            case NUM_SEGMENTS:
               builder.clustering().hash().numSegments(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case GROUPS:
               parseGroups(reader, holder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }

   }

   private void parseGroups(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {

      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      ParseUtils.requireSingleAttribute(reader, "enabled");

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
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
               builder.clustering().hash().groups().addGrouper(Util.<Grouper<?>>getInstance(value, holder.getClassLoader()));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }

   }

   private void parseAsync(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case ASYNC_MARSHALLING:
               if (Boolean.parseBoolean(value)) {
                  builder.clustering().async().asyncMarshalling();
               } else {
                  builder.clustering().async().syncMarshalling();
               }
               break;
            case REPL_QUEUE_CLASS:
               builder.clustering().async().replQueue(Util.<ReplicationQueue> getInstance(value, holder.getClassLoader()));
               break;
            case REPL_QUEUE_INTERVAL:
               builder.clustering().async().replQueueInterval(Long.parseLong(value));
               break;
            case REPL_QUEUE_MAX_ELEMENTS:
               builder.clustering().async().replQueueMaxElements(Integer.parseInt(value));
               break;
            case USE_REPL_QUEUE:
               builder.clustering().async().useReplQueue(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseGlobal(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      ParseUtils.requireNoAttributes(reader);
      boolean transportParsed = false;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ASYNC_LISTENER_EXECUTOR: {
               parseAsyncListenerExecutor(reader, holder);
               break;
            }
            case ASYNC_TRANSPORT_EXECUTOR: {
               parseAsyncTransportExecutor(reader, holder);
               break;
            }
            case EVICTION_SCHEDULED_EXECUTOR: {
               parseEvictionScheduledExecutor(reader, holder);
               break;
            }
            case GLOBAL_JMX_STATISTICS: {
               parseGlobalJMXStatistics(reader, holder);
               break;
            }
            case MODULES: {
               parseModules(reader, holder);
               break;
            }
            case REPLICATION_QUEUE_SCHEDULED_EXECUTOR: {
               parseReplicationQueueScheduledExecutor(reader, holder);
               break;
            }
            case SERIALIZATION: {
               parseSerialization(reader, holder);
               break;
            }
            case SHUTDOWN: {
               parseShutdown(reader, holder);
               break;
            }
            case TRANSPORT: {
               parseTransport(reader, holder);
               transportParsed = true;
               break;
            } case SITE: {
               parseGlobalSites(reader, holder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

      if (!transportParsed) {
         // make sure there is no "default" transport
         builder.transport().transport(null);
      } else {
         // The transport *has* been parsed.  If we don't have a transport set, make sure we set the default.
         if (builder.transport().getTransport() == null) {
            builder.transport().defaultTransport();
         }
      }
   }

   private void parseTransport(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLUSTER_NAME: {
               builder.transport().clusterName(value);
               break;
            }
            case DISTRIBUTED_SYNC_TIMEOUT: {
               builder.transport().distributedSyncTimeout(Long.parseLong(value));
               break;
            }
            case MACHINE_ID: {
               builder.transport().machineId(value);
               break;
            }
            case NODE_NAME: {
               builder.transport().nodeName(value);
               break;
            }
            case RACK_ID: {
               builder.transport().rackId(value);
               break;
            }
            case SITE_ID: {
               builder.transport().siteId(value);
               break;
            }
            case STRICT_PEER_TO_PEER: {
               builder.transport().strictPeerToPeer(Boolean.valueOf(value));
               break;
            }
            case TRANSPORT_CLASS: {
               builder.transport().transport(Util.<Transport> getInstance(value, holder.getClassLoader()));
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
               builder.transport().withProperties(parseProperties(reader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseShutdown(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case HOOK_BEHAVIOR: {
               builder.shutdown().hookBehavior(ShutdownHookBehavior.valueOf(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseSerialization(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case MARSHALLER_CLASS: {
               builder.serialization().marshaller(Util.<Marshaller>getInstance(value, holder.getClassLoader()));
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

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ADVANCED_EXTERNALIZERS: {
               parseAdvancedExternalizers(reader, holder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

   }

   private void parseAdvancedExternalizers(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      ParseUtils.requireNoAttributes(reader);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ADVANCED_EXTERNALIZER: {
               int attributes = reader.getAttributeCount();
               AdvancedExternalizer<?> advancedExternalizer = null;
               Integer id = null;
               ParseUtils.requireAttributes(reader, Attribute.EXTERNALIZER_CLASS.getLocalName());
               for (int i = 0; i < attributes; i++) {
                  String value = replaceProperties(reader.getAttributeValue(i));
                  Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
                  switch (attribute) {
                     case EXTERNALIZER_CLASS: {
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

   private void parseReplicationQueueScheduledExecutor(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FACTORY: {
               builder.replicationQueueScheduledExecutor().factory(Util.<ScheduledExecutorFactory> getInstance(value, holder.getClassLoader()));
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
               builder.replicationQueueScheduledExecutor().withProperties(parseProperties(reader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseGlobalJMXStatistics(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         // allowDuplicateDomains="true" cacheManagerName="" enabled="true" jmxDomain=""
         // mBeanServerLookup
         switch (attribute) {
            case ALLOW_DUPLICATE_DOMAINS: {
               builder.globalJmxStatistics().allowDuplicateDomains(Boolean.valueOf(value));
               break;
            }
            case CACHE_MANAGER_NAME: {
               builder.globalJmxStatistics().cacheManagerName(value);
               break;
            }
            case ENABLED: {
               if (!Boolean.parseBoolean(value)) {
                  builder.globalJmxStatistics().disable();
               } else {
                  builder.globalJmxStatistics().enable();
               }
               break;
            }
            case JMX_DOMAIN: {
               builder.globalJmxStatistics().jmxDomain(value);
               break;
            }
            case MBEAN_SERVER_LOOKUP: {
               builder.globalJmxStatistics().mBeanServerLookup(Util.<MBeanServerLookup> getInstance(value, holder.getClassLoader()));
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
               builder.globalJmxStatistics().withProperties(parseProperties(reader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseEvictionScheduledExecutor(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FACTORY: {
               builder.evictionScheduledExecutor().factory(Util.<ScheduledExecutorFactory> getInstance(value, holder.getClassLoader()));
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
               builder.evictionScheduledExecutor().withProperties(parseProperties(reader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseAsyncTransportExecutor(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FACTORY: {
               builder .asyncTransportExecutor().factory(Util.<ExecutorFactory> getInstance(value, holder.getClassLoader()));
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
               builder.asyncTransportExecutor().withProperties(parseProperties(reader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseAsyncListenerExecutor(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FACTORY: {
               builder.asyncListenerExecutor().factory(Util.<ExecutorFactory> getInstance(value, holder.getClassLoader()));
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
               builder.asyncListenerExecutor().withProperties(parseProperties(reader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

   }

   public static Properties parseProperties(final XMLExtendedStreamReader reader) throws XMLStreamException {

      ParseUtils.requireNoAttributes(reader);

      Properties p = new Properties();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case PROPERTY: {
               int attributes = reader.getAttributeCount();
               ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName(), Attribute.VALUE.getLocalName());
               String key = null;
               String propertyValue = null;
               for (int i = 0; i < attributes; i++) {
                  String value = replaceProperties(reader.getAttributeValue(i));
                  Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
                  switch (attribute) {
                     case NAME: {
                        key = value;
                        break;
                     } case VALUE: {
                        propertyValue = value;
                        break;
                     }
                     default: {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                     }
                  }
               }
               p.put(key, propertyValue);

               ParseUtils.requireNoContent(reader);

               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      return p;
   }

}
