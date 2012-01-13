package org.infinispan.configuration.parsing;

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.AbstractLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.configuration.cache.LoaderConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.file.FileCacheStore;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.FileLookup;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.StringPropertyReplacer;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

import static org.infinispan.configuration.cache.CacheMode.*;

public class Parser {
   
   private static final Log log = LogFactory.getLog(Parser.class); 

   private static void safeClose(final Closeable closeable) {
      if (closeable != null) try {
          closeable.close();
      } catch (Throwable t) {
          log.failedToCloseResource(t);
      }
   }
   
   private static String replaceSystemProperties(String value) {
      int dollar = value.indexOf('$');
      if (dollar >= 0 && value.indexOf('{', dollar) > 0 && value.indexOf('}', dollar) > 0) {
         String replacedValue = StringPropertyReplacer.replaceProperties(value);
         if (value.equals(replacedValue)) {
            log.propertyCouldNotBeReplaced(value.substring(value.indexOf('{') + 1, value.indexOf('}')));
         }
         return replacedValue;
      } else {
         return value;
      }
   }
   
   private final ClassLoader cl;

   public Parser(ClassLoader cl) {
      this.cl = cl;
   }

   public ConfigurationBuilderHolder parse(String filename) {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      return parse(fileLookup.lookupFile(filename, cl));
   }
   
   public ConfigurationBuilderHolder parse(InputStream is) {
      try {
         try {
             BufferedInputStream input = new BufferedInputStream(is);
             XMLStreamReader streamReader = XMLInputFactory.newInstance().createXMLStreamReader(input);
             ConfigurationBuilderHolder holder = doParse(streamReader);
             streamReader.close();
             input.close();
             is.close();
             return holder;
         } finally {
             safeClose(is);
         }
      } catch (ConfigurationException e) {
         throw e;
      } catch (Exception e) {
            throw new ConfigurationException(e);
      }
   }
   
   private ConfigurationBuilderHolder doParse(XMLStreamReader reader) throws XMLStreamException {

      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      
      Element root = ParseUtils.nextElement(reader);
      
      if (!root.getLocalName().equals(Element.ROOT.getLocalName())) {
         throw ParseUtils.missingRequiredElement(reader, Collections.singleton(Element.ROOT));
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case DEFAULT: {
               parseDefaultCache(reader, holder.getDefaultConfigurationBuilder());
               break;
            }
            case GLOBAL: {
               parseGlobal(reader, holder.getGlobalConfigurationBuilder());
               break;
            }
            case NAMED_CACHE: {
               parseNamedCache(reader, holder.newConfigurationBuilder());
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      return holder;
   }

   private void parseNamedCache(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      
      ParseUtils.requireSingleAttribute(reader, Attribute.NAME.getLocalName());
      
      String name = "";
      
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               name = value;
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      builder.name(name);
      parseCache(reader, builder);
      
   }

   private void parseDefaultCache(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      parseCache(reader, builder);
   }

   private void parseCache(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLUSTERING:
               parseClustering(reader, builder);
               break;
            case CUSTOM_INTERCEPTORS:
               parseCustomInterceptors(reader, builder);
               break;
            case DATA_CONTAINER:
               parseDataContainer(reader, builder);
               break;
            case DEADLOCK_DETECTION:
               parseDeadlockDetection(reader, builder);
               break;
            case EVICTION:
               parseEviction(reader, builder);
               break;
            case EXPIRATION:
               parseExpiration(reader, builder);
               break;
            case INDEXING:
               parseIndexing(reader, builder);
               break;
            case INVOCATION_BATCHING:
               parseInvocationBatching(reader, builder);
               break;
            case JMX_STATISTICS:
               parseJmxStatistics(reader, builder);
               break;
            case LOADERS:
               parseLoaders(reader, builder);
               break;
            case LOCKING:
               parseLocking(reader, builder);
               break;
            case LAZY_DESERIALIZATION:
            case STORE_AS_BINARY:
               parseStoreAsBinary(reader, builder);
               break;
            case TRANSACTION:
               parseTransaction(reader, builder);
               break;
            case UNSAFE:
               parseUnsafe(reader, builder);
               break;
            case VERSIONING:
               parseVersioning(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseVersioning(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      builder.versioning().disable(); // Disabled by default.
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
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
   private void parseTransaction(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      boolean forceSetTransactional = false;
      boolean transactionModeSpecified = false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case AUTO_COMMIT:
               builder.transaction().autoCommit(Boolean.valueOf(value));
               break;
            case CACHE_STOP_TIMEOUT:
               builder.transaction().cacheStopTimeout(Integer.valueOf(value));
               break;
            case EAGER_LOCK_SINGLE_NODE:
               builder.transaction().eagerLockingSingleNode(Boolean.valueOf(value));
               break;
            case LOCKING_MODE:
               builder.transaction().lockingMode(LockingMode.valueOf(value));
               break;
            case SYNC_COMMIT_PHASE:
               builder.transaction().syncCommitPhase(Boolean.valueOf(value));
               break;
            case SYNC_ROLLBACK_PHASE:
               builder.transaction().syncRollbackPhase(Boolean.valueOf(value));
               break;
            case TRANSACTION_MANAGER_LOOKUP_CLASS:
               builder.transaction().transactionManagerLookup(Util.<TransactionManagerLookup>getInstance(value, cl));
               forceSetTransactional = true;
               break;
            case TRANSACTION_MODE:
               builder.transaction().transactionMode(TransactionMode.valueOf(value));
               transactionModeSpecified = true;
               break;
            case USE_EAGER_LOCKING:
               builder.transaction().useEagerLocking(Boolean.valueOf(value));
               break;
            case USE_SYNCHRONIZAION:
               builder.transaction().useSynchronization(Boolean.valueOf(value));
               break;
            case USE_1PC_FOR_AUTOCOMMIT_TX:
               builder.transaction().use1PcForAutoCommitTransactions(Boolean.valueOf(value));
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      if (!transactionModeSpecified && forceSetTransactional) builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case RECOVERY:
               parseRecovery(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      
   }

   private void parseRecovery(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  builder.transaction().recovery().enable();
               else
                  builder.transaction().recovery().disable();
               break;
            case RECOVERY_INFO_CACHE_NAME:
               builder.transaction().recovery().recoveryInfoCacheName(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
   }

   private void parseUnsafe(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case UNRELIABLE_RETURN_VALUES:
               builder.unsafe().unreliableReturnValues(Boolean.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
      
   }

   private void parseStoreAsBinary(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  builder.storeAsBinary().enable();
               else
                  builder.storeAsBinary().disable();
               break;
            case STORE_KEYS_AS_BINARY:
               builder.storeAsBinary().storeKeysAsBinary(Boolean.valueOf(value));
               break;
            case STORE_VALUES_AS_BINARY:
               builder.storeAsBinary().storeValuesAsBinary(Boolean.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
      
   }

   private void parseLocking(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CONCURRENCY_LEVEL:
               builder.locking().concurrencyLevel(Integer.valueOf(value));
               break;
            case ISOLATION_LEVEL:
               builder.locking().isolationLevel(IsolationLevel.valueOf(value));
               break;
            case LOCK_ACQUISITION_TIMEOUT:
               builder.locking().lockAcquisitionTimeout(Long.valueOf(value));
               break;
            case USE_LOCK_STRIPING:
               builder.locking().useLockStriping(Boolean.valueOf(value));
               break;
            case WRITE_SKEW_CHECK:
               builder.locking().writeSkewCheck(Boolean.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
      
   }

   private void parseLoaders(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PASSIVATION:
               builder.loaders().passivation(Boolean.valueOf(value));
               break;
            case PRELOAD:
               builder.loaders().preload(Boolean.valueOf(value));
               break;
            case SHARED:
               builder.loaders().shared(Boolean.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case LOADER:
               parseLoader(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseLoader(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      CacheLoader loader = null;
      Boolean fetchPersistentState = null;
      Boolean ignoreModifications = null;
      Boolean purgeOnStartup = null;
      Integer purgerThreads = null;
      Boolean purgeSynchronously = null;
      
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS:
               loader = Util.getInstance(value, cl);
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
            parseLoaderChildren(reader, fcscb);
         } else {
            LoaderConfigurationBuilder lcb = builder.loaders().addCacheLoader();
            lcb.cacheLoader(loader);
            if (fetchPersistentState != null)
               lcb.fetchPersistentState(fetchPersistentState);
            if (ignoreModifications != null)
               lcb.ignoreModifications(ignoreModifications);
            if (purgerThreads != null)
               lcb.purgerThreads(purgerThreads);
            if (purgeOnStartup != null)
               lcb.purgeOnStartup(purgeOnStartup);
            if (purgeSynchronously != null)
               lcb.purgeSynchronously(purgeSynchronously);
            parseLoaderChildren(reader, lcb);
         }
         
      }
      
   }
   
   private void parseLoaderChildren(XMLStreamReader reader, AbstractLoaderConfigurationBuilder<?> loaderBuilder) throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ASYNC:
               parseAsyncLoader(reader, loaderBuilder);
               break;
            case PROPERTIES:
               loaderBuilder.withProperties(parseProperties(reader));
               break;
            case SINGLETON_STORE:
               parseSingletonStore(reader, loaderBuilder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSingletonStore(XMLStreamReader reader, AbstractLoaderConfigurationBuilder<?> loaderBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  loaderBuilder.singletonStore().enable();
               else
                  loaderBuilder.singletonStore().disable();
               break;
            case PUSH_STATE_TIMEOUT:
               loaderBuilder.singletonStore().pushStateTimeout(Long.valueOf(value));
               break;
            case PUSH_STATE_WHEN_COORDINATOR:
               loaderBuilder.singletonStore().pushStateWhenCoordinator(Boolean.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
   }

   private void parseAsyncLoader(XMLStreamReader reader, AbstractLoaderConfigurationBuilder loaderBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  loaderBuilder.async().enable();
               else
                  loaderBuilder.async().disable();
               break;
            case FLUSH_LOCK_TIMEOUT:
               loaderBuilder.async().flushLockTimeout(Long.valueOf(value));
               break;
            case MODIFICTION_QUEUE_SIZE:
               loaderBuilder.async().modificationQueueSize(Integer.valueOf(value));
               break;
            case SHUTDOWN_TIMEOUT:
               loaderBuilder.async().shutdownTimeout(Long.valueOf(value));
               break;
            case THREAD_POOL_SIZE:
               loaderBuilder.async().threadPoolSize(Integer.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
      
   }

   private void parseJmxStatistics(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  builder.jmxStatistics().enable();
               else
                  builder.jmxStatistics().disable();
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
   }

   private void parseInvocationBatching(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  builder.invocationBatching().enable();
               else
                  builder.invocationBatching().disable();
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
      
   }

   private void parseIndexing(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  builder.indexing().enable();
               else
                  builder.indexing().disable();
               break;
            case INDEX_LOCAL_ONLY:
                  builder.indexing().indexLocalOnly(Boolean.valueOf(value));
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

   private void parseExpiration(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case LIFESPAN:
               builder.expiration().lifespan(Long.valueOf(value));
               break;
            case MAX_IDLE:
               builder.expiration().maxIdle(Long.valueOf(value));
               break;
            case REAPER_ENABLED:
               if (Boolean.valueOf(value))
                  builder.expiration().enableReaper();
               else
                  builder.expiration().disableReaper();
               break;
            case WAKE_UP_INTERVAL:
               builder.expiration().wakeUpInterval(Long.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
      
   }

   private void parseEviction(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case MAX_ENTRIES:
               builder.eviction().maxEntries(Integer.valueOf(value));
               break;
            case STRATEGY:
               builder.eviction().strategy(EvictionStrategy.valueOf(value));
               break;
            case THREAD_POLICY:
               builder.eviction().threadPolicy(EvictionThreadPolicy.valueOf(value));
               break;
            case WAKE_UP_INTERVAL:
               final Long wakeUpInterval = Long.valueOf(value);
               log.evictionWakeUpIntervalDeprecated(wakeUpInterval);
               builder.expiration().wakeUpInterval(wakeUpInterval);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
      
   }

   private void parseDeadlockDetection(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  builder.deadlockDetection().enable();
               else
                  builder.deadlockDetection().disable();
               break;
            case SPIN_DURATION:
               builder.deadlockDetection().spinDuration(Long.valueOf(value).intValue());
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
      
   }

   private void parseDataContainer(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS:
               builder.dataContainer().dataContainer(Util.<DataContainer>getInstance(value, cl));
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

   private void parseCustomInterceptors(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case INTERCEPTOR:
               parseInterceptor(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      
   }

   private void parseInterceptor(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      
      InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();
      
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case AFTER:
               interceptorBuilder.after(Util.<CommandInterceptor>loadClass(value, cl));
               break;
            case BEFORE:
               interceptorBuilder.before(Util.<CommandInterceptor>loadClass(value, cl));
               break;
            case CLASS:
               interceptorBuilder.interceptor(Util.<CommandInterceptor>getInstance(value, cl));
               break;
            case INDEX:
               interceptorBuilder.index(Integer.valueOf(value));
               break;
            case POSITION:
               interceptorBuilder.position(Position.valueOf(value.toUpperCase()));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
   }

   private void parseClustering(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {

      String clusteringMode = null;
      boolean synchronous = false;
      boolean asynchronous = false;
      
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
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
               parseAsync(reader, builder);
               break;
            case HASH:
               parseHash(reader, builder);
               break;
            case L1:
               parseL1reader(reader, builder);
               break;
            case STATE_RETRIEVAL:
               parseStateRetrieval(reader, builder);
               break;
            case SYNC:
               synchronous = true;
               setMode(builder, clusteringMode, asynchronous, asynchronous, reader);
               parseSync(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      
      if (!synchronous && !asynchronous)
         setMode(builder, clusteringMode, asynchronous, asynchronous, reader);
      
      
   }
   
   private void setMode(ConfigurationBuilder builder, String clusteringMode, boolean asynchronous, boolean synchronous, XMLStreamReader reader) {
      if (synchronous && asynchronous) 
         throw new ConfigurationException("Cannot configure <sync> and <async> on the same cluster, " + reader.getLocation());
      
      if (clusteringMode != null) {
         String mode = clusteringMode.toUpperCase();
         if (mode.startsWith("R")) {
            if (!asynchronous)
              builder.clustering().cacheMode(REPL_SYNC);
            else
               builder.clustering().cacheMode(REPL_ASYNC);
         } else if (mode.startsWith("I")) {
            if (!asynchronous)
               builder.clustering().cacheMode(INVALIDATION_SYNC);
            else
               builder.clustering().cacheMode(INVALIDATION_ASYNC);
         } else if (mode.startsWith("D")) {
            if (!asynchronous)
               builder.clustering().cacheMode(DIST_SYNC);
            else
               builder.clustering().cacheMode(DIST_ASYNC);
         } else if (mode.startsWith("L")) {
            builder.clustering().cacheMode(LOCAL);
         } else {
            throw new ConfigurationException("Invalid clustering mode " + clusteringMode + ", " + reader.getLocation());
         }
      }
   }

   private void parseSync(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case REPL_TIMEOUT:
               builder.clustering().sync().replTimeout(Long.valueOf(value));
               break;
           
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);
      
   }

   private void parseStateRetrieval(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException{

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ALWAYS_PROVIDE_IN_MEMORY_STATE:
               builder.clustering().stateRetrieval().alwaysProvideInMemoryState(Boolean.valueOf(value));
               break;
            case FETCH_IN_MEMORY_STATE:
               builder.clustering().stateRetrieval().fetchInMemoryState(Boolean.valueOf(value));
               break;
            case INITIAL_RETRY_WAIT_TIME:
               builder.clustering().stateRetrieval().initialRetryWaitTime(Long.valueOf(value));
               break;
            case LOG_FLUSH_TIMEOUT:
               builder.clustering().stateRetrieval().logFlushTimeout(Long.valueOf(value));
               break;
            case MAX_NON_PROGRESSING_LOG_WRITES:
               builder.clustering().stateRetrieval().maxNonProgressingLogWrites(Integer.valueOf(value));
               break;
            case NUM_RETRIES:
               builder.clustering().stateRetrieval().numRetries(Integer.valueOf(value));
               break;
            case RETRY_WAIT_TIME_INCREASE_FACTOR:
               builder.clustering().stateRetrieval().retryWaitTimeIncreaseFactor(Integer.valueOf(value));
               break;
            case TIMEOUT:
               builder.clustering().stateRetrieval().timeout(Long.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);

   }

   private void parseL1reader(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  builder.clustering().l1().enable();
               else
                  builder.clustering().l1().disable();
               break;
            case INVALIDATION_THRESHOLD:
               builder.clustering().l1().invalidationThreshold(Integer.valueOf(value));
               break;
            case LIFESPAN:
               builder.clustering().l1().lifespan(Long.valueOf(value));
               break;
            case ON_REHASH:
               if (Boolean.valueOf(value))
                  builder.clustering().l1().enableOnRehash();
               else
                  builder.clustering().l1().disableOnRehash();
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      
      ParseUtils.requireNoContent(reader);

   }

   private void parseHash(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLASS:
            case HASH_FUNCTION_CLASS:
               builder.clustering().hash().consistentHash(Util.<ConsistentHash> getInstance(value, cl));
               break;
            case NUM_OWNERS:
               builder.clustering().hash().numOwners(Integer.valueOf(value));
               break;
            case NUM_VIRTUAL_NODES:
               builder.clustering().hash().numVirtualNodes(Integer.valueOf(value));
               break;
            case REHASH_ENABLED:
               if (Boolean.valueOf(value))
                  builder.clustering().hash().rehashEnabled();
               else
                  builder.clustering().hash().rehashDisabled();
               break;
            case REHASH_RPC_TIMEOUT:
               builder.clustering().hash().rehashRpcTimeout(Long.valueOf(value));
               break;
            case REHASH_WAIT:
               builder.clustering().hash().rehashWait(Long.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case GROUPS:
               parseGroups(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }

   }

   private void parseGroups(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {

      ParseUtils.requireSingleAttribute(reader, "enabled");

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.valueOf(value))
                  builder.clustering().hash().groups().enabled();
               else
                  builder.clustering().hash().groups().disabled();
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
               builder.clustering().hash().groups().addGrouper(Util.<Grouper<?>>getInstance(value, cl));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }

   }

   private void parseAsync(XMLStreamReader reader, ConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ASYNC_MARSHALLING:
               if (Boolean.valueOf(value))
                  builder.clustering().async().asyncMarshalling();
               else
                  builder.clustering().async().syncMarshalling();
               break;
            case REPL_QUEUE_CLASS:
               builder.clustering().async().replQueue(Util.<ReplicationQueue> getInstance(value, cl));
               break;
            case REPL_QUEUE_INTERVAL:
               builder.clustering().async().replQueueInterval(Long.valueOf(value));
               break;
            case REPL_QUEUE_MAX_ELEMENTS:
               builder.clustering().async().replQueueMaxElements(Integer.valueOf(value));
               break;
            case USE_REPL_QUEUE:
               builder.clustering().async().useReplQueue(Boolean.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);

   }

   private void parseGlobal(XMLStreamReader reader, GlobalConfigurationBuilder builder) throws XMLStreamException {

      ParseUtils.requireNoAttributes(reader);
      boolean transportParsed = false;
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ASYNC_LISTENER_EXECUTOR: {
               parseAsyncListenerExectuor(reader, builder);
               break;
            }
            case ASYNC_TRANSPORT_EXECUTOR: {
               parseAsyncTransportExecutor(reader, builder);
               break;
            }
            case EVICTION_SCHEDULED_EXECUTOR: {
               parseEvictionScheduledExecutor(reader, builder);
               break;
            }
            case GLOBAL_JMX_STATISTICS: {
               parseGlobalJMXStatistics(reader, builder);
               break;
            }
            case REPLICATION_QUEUE_SCHEDULED_EXECUTOR: {
               parseReplicationQueueScheduledExecutor(reader, builder);
               break;
            }
            case SERIALIZATION: {
               parseSerialization(reader, builder);
               break;
            }
            case SHUTDOWN: {
               parseShutdown(reader, builder);
               break;
            }
            case TRANSPORT: {
               parseTransport(reader, builder);
               transportParsed = true;
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
            builder.transport().transport(Util.getInstance(TransportConfigurationBuilder.DEFAULT_TRANSPORT));
         }
      }
   }

   private void parseTransport(XMLStreamReader reader, GlobalConfigurationBuilder builder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CLUSTER_NAME: {
               builder.transport().clusterName(value);
               break;
            }
            case DISTRIBUTED_SYNC_TIMEOUT: {
               builder.transport().distributedSyncTimeout(Long.valueOf(value));
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
               builder.transport().transport(Util.<Transport> getInstance(value, cl));
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

   private void parseShutdown(XMLStreamReader reader, GlobalConfigurationBuilder builder) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
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

   private void parseSerialization(XMLStreamReader reader, GlobalConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case MARSHALLER_CLASS: {
               builder.serialization().marshallerClass(Util.<Marshaller> loadClass(value, cl));
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
               parseAdvancedExternalizers(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

   }

   private void parseAdvancedExternalizers(XMLStreamReader reader, GlobalConfigurationBuilder builder)
         throws XMLStreamException {

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
                  String value = replaceSystemProperties(reader.getAttributeValue(i));
                  Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
                  switch (attribute) {
                     case EXTERNALIZER_CLASS: {
                        advancedExternalizer = Util.getInstance(value, cl);
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
               
               if (id != null)
                  builder.serialization().addAdvancedExternalizer(id, advancedExternalizer);
               else
                  builder.serialization().addAdvancedExternalizer(advancedExternalizer);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private void parseReplicationQueueScheduledExecutor(XMLStreamReader reader, GlobalConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FACTORY: {
               builder.replicationQueueScheduledExecutor().factory(Util.<ScheduledExecutorFactory> getInstance(value, cl));
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

   private void parseGlobalJMXStatistics(XMLStreamReader reader, GlobalConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
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
               if (!Boolean.valueOf(value))
                  builder.globalJmxStatistics().disable();
               else
                  builder.globalJmxStatistics().enable();
               break;
            }
            case JMX_DOMAIN: {
               builder.globalJmxStatistics().jmxDomain(value);
               break;
            }
            case MBEAN_SERVER_LOOKUP: {
               builder.globalJmxStatistics().mBeanServerLookup(Util.<MBeanServerLookup> getInstance(value, cl));
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

   private void parseEvictionScheduledExecutor(XMLStreamReader reader, GlobalConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FACTORY: {
               builder.evictionScheduledExecutor().factory(Util.<ScheduledExecutorFactory> getInstance(value, cl));
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

   private void parseAsyncTransportExecutor(XMLStreamReader reader, GlobalConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FACTORY: {
               builder.asyncTransportExecutor().factory(Util.<ExecutorFactory> getInstance(value, cl));
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

   private void parseAsyncListenerExectuor(XMLStreamReader reader, GlobalConfigurationBuilder builder)
         throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceSystemProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case FACTORY: {
               builder.asyncListenerExecutor().factory(Util.<ExecutorFactory> getInstance(value, cl));
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

   public static Properties parseProperties(XMLStreamReader reader) throws XMLStreamException {

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
                  String value = replaceSystemProperties(reader.getAttributeValue(i));
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
