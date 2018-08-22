package org.infinispan.tools;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;

import org.infinispan.Version;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.interceptors.FooInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.ManagedConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.SimpleConnectionFactoryConfiguration;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration;
import org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.rest.configuration.RestStoreConfiguration;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tools.config.ConfigurationConverter;
import org.infinispan.tools.customs.CustomDataContainer;
import org.infinispan.tools.customs.CustomTransport;
import org.testng.annotations.Test;

@Test(testName = "tools.ConfigurationConverterTest", groups = "functional")
public class ConfigurationConverterTest extends AbstractInfinispanTest {

   public static final String SERIALIZED_CONFIG_FILE_NAME = "target/serialized_config.xml";

   public void testConversionFrom60() throws Exception {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ConfigurationConverter.convert(ConfigurationConverterTest.class.getResourceAsStream("/6.0.xml"), baos);
      ParserRegistry pr = new ParserRegistry();
      pr.parse(new ByteArrayInputStream(baos.toByteArray()));
   }

   public void testConversionAndSerializationFrom60() throws Exception {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ConfigurationConverter.convert(ConfigurationConverterTest.class.getResourceAsStream("/6.0.xml"), baos);

      try(OutputStream outputStream = new FileOutputStream(SERIALIZED_CONFIG_FILE_NAME)) {
         baos.writeTo(outputStream);
      }

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(SERIALIZED_CONFIG_FILE_NAME, true, false, false)) {

         @Override
         public void call() {
            assertGlobalPropertiesConverted(cm);
            assertDefaultConfigApplied(cm);
            assertDataContainerConverted(cm);
            assertIndexingConverted(cm);
            assertTransactionConverted(cm);
            assertLockingConverted(cm);
            assertCompatibilityConverted(cm);
            assertBackupsConverted(cm);
            assertExpirationEvictionConverted(cm);
            assertCustomInterceptorsConverted(cm);
            assertDeadlockDetectionConverted(cm);
            assertJmxStatisticsConverted(cm);
            assertStoreAsBinaryConverted(cm);
            assertClusteringConverted(cm);
            assertPersistenceConverted(cm);
            assertUnsafeConverted(cm);
         }

      });
   }

   private void assertGlobalPropertiesConverted(EmbeddedCacheManager cm) {
      GlobalConfiguration globalConfiguration = cm.getCacheManagerConfiguration();
      assertEquals("infinispan-cluster", globalConfiguration.transport().clusterName());
      assertEquals("r1", globalConfiguration.transport().rackId());
      assertEquals("m1", globalConfiguration.transport().machineId());
      assertEquals("s1", globalConfiguration.transport().siteId());
      assertTrue(globalConfiguration.transport().transport() instanceof CustomTransport);
      assertEquals("s1", globalConfiguration.sites().localSite());

      TypedProperties props = globalConfiguration.transport().properties();
      boolean stackVerified = false;
      for (String name: props.stringPropertyNames()) {
         if (name.startsWith("stackFilePath-")) {
            assertEquals("jgroups-udp.xml", props.get(name));
            stackVerified = true;
         }
      }
      if (!stackVerified) {
         fail("The stack verification failed! No stack element present.");
      }

      assertTransportFactories(globalConfiguration);
      assertGlobalSerialization(globalConfiguration);
      assertGlobalExecutorsConverted(globalConfiguration);

      assertTrue(globalConfiguration.globalJmxStatistics().allowDuplicateDomains());
      assertTrue(globalConfiguration.globalJmxStatistics().mbeanServerLookup() instanceof org.infinispan.commons.jmx.PerThreadMBeanServerLookup);
      assertTrue(globalConfiguration.globalJmxStatistics().enabled());
      assertEquals("funky_domain", globalConfiguration.globalJmxStatistics().domain());
      assertEquals("TestCacheManager", globalConfiguration.globalJmxStatistics().cacheManagerName());
      assertEquals("testValue", globalConfiguration.globalJmxStatistics().properties().get("testKey"));
      assertEquals("REGISTER", globalConfiguration.shutdown().hookBehavior().name());
   }

   private void assertTransportFactories(GlobalConfiguration gb) {
      DefaultThreadFactory threadFactory;
      BlockingThreadPoolExecutorFactory threadPool;

      threadFactory = gb.transport().remoteCommandThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(1, threadFactory.initialPriority());
      threadPool = gb.transport().remoteCommandThreadPool().threadPoolFactory();
      assertEquals(TestCacheManagerFactory.REMOTE_EXEC_MAX_THREADS, threadPool.coreThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.REMOTE_EXEC_MAX_THREADS, threadPool.maxThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.REMOTE_EXEC_QUEUE_SIZE, threadPool.queueLength()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.KEEP_ALIVE, threadPool.keepAlive());  // overriden by TestCacheManagerFactory

      threadFactory = gb.transport().transportThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(1, threadFactory.initialPriority());
      threadPool = gb.transport().transportThreadPool().threadPoolFactory();
      assertEquals(TestCacheManagerFactory.TRANSPORT_EXEC_MAX_THREADS, threadPool.coreThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.TRANSPORT_EXEC_MAX_THREADS, threadPool.maxThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.TRANSPORT_EXEC_QUEUE_SIZE, threadPool.queueLength()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.KEEP_ALIVE, threadPool.keepAlive());  // overriden by TestCacheManagerFactory
   }

   private void assertGlobalSerialization(GlobalConfiguration globalConfiguration) {
      assertTrue(globalConfiguration.serialization().marshaller() instanceof TestObjectStreamMarshaller);
      assertEquals(Version.getVersionShort("1.0.0"), globalConfiguration.serialization().version());
      assertEquals(3, globalConfiguration.serialization().advancedExternalizers().size());
      assertTrue(globalConfiguration.serialization().advancedExternalizers().get(1234) instanceof org.infinispan.marshall.AdvancedExternalizerTest.IdViaConfigObj.Externalizer);
      assertTrue(globalConfiguration.serialization().advancedExternalizers().get(3456) instanceof org.infinispan.marshall.AdvancedExternalizerTest.IdViaBothObj.Externalizer);
      assertTrue(globalConfiguration.serialization().advancedExternalizers().get(5678) instanceof org.infinispan.marshall.AdvancedExternalizerTest.IdViaAnnotationObj.Externalizer);
   }

   private void assertGlobalExecutorsConverted(GlobalConfiguration globalConfiguration) {
      DefaultThreadFactory threadFactory;
      BlockingThreadPoolExecutorFactory threadPool;

      //AsyncListener ThreadPool Check
      threadFactory = globalConfiguration.listenerThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(1, threadFactory.initialPriority());

      threadPool = globalConfiguration.listenerThreadPool().threadPoolFactory();
      assertEquals(2, threadPool.coreThreads());
      assertEquals(5, threadPool.maxThreads());
      assertEquals(12000, threadPool.queueLength());
      assertEquals(60000, threadPool.keepAlive());

      //Persistence ThreadPool Check
      threadFactory = globalConfiguration.persistenceThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(1, threadFactory.initialPriority());

      threadPool = globalConfiguration.persistenceThreadPool().threadPoolFactory();
      assertEquals(6, threadPool.coreThreads());
      assertEquals(6, threadPool.maxThreads());
      assertEquals(10001, threadPool.queueLength());
      assertEquals(60000, threadPool.keepAlive());

      //ExpirationExecutor ThreadPoolCheck
      threadFactory = globalConfiguration.expirationThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(1, threadFactory.initialPriority());

      assertNotNull(globalConfiguration.expirationThreadPool().threadPoolFactory());
   }

   private void assertDataContainerConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("withDataContainer");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.dataContainer().dataContainer() instanceof CustomDataContainer);

      // Equivalence is ignored
      assertTrue(config.dataContainer().<byte[]>valueEquivalence() instanceof AnyEquivalence);
      // Equivalence is ignored
      assertTrue(config.dataContainer().<byte[]>keyEquivalence() instanceof AnyEquivalence);
   }

   private void assertDefaultConfigApplied(EmbeddedCacheManager cm) {
      for (String cacheName : cm.getCacheNames()) {
         Configuration config = cm.getCacheConfiguration(cacheName);
         if (!cacheName.startsWith("transaction") && !cacheName.startsWith("tx")) {
            assertFalse("Assertion failed for cache: " + cacheName, config.transaction().transactionMode().isTransactional());
            assertEquals(123, config.transaction().reaperWakeUpInterval());
            assertEquals(3123, config.transaction().completedTxTimeout());
         }

         if (!cacheName.startsWith("locking")) {
            assertEquals(1000, config.locking().lockAcquisitionTimeout());
            assertEquals(100, config.locking().concurrencyLevel());
         }

         if (!cacheName.equals("jmxEnabled"))
            assertFalse(config.jmxStatistics().enabled());
         else
            assertTrue(config.jmxStatistics().enabled());
      }
   }

   private void assertIndexingConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("withIndexingNotLocal");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals("ALL", config.indexing().index().name());
      assertEquals("test1", config.indexing().properties().get("test"));

      config = cm.getCacheConfiguration("withIndexingLocalOnly");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals("LOCAL", config.indexing().index().name());
      assertEquals("test1", config.indexing().properties().get("test"));

      config = cm.getCacheConfiguration("withDisabledIndexing");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals("NONE", config.indexing().index().name());
   }

   private void assertTransactionConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("transactionalWithInvocationBatching");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.transaction().transactionMode().isTransactional());
      assertEquals("TRANSACTIONAL", config.transaction().transactionMode().name());
      assertTrue(config.transaction().useSynchronization());
      assertFalse(config.transaction().recovery().enabled());
      assertTrue(config.transaction().autoCommit());
      assertTrue(config.invocationBatching().enabled());

      config = cm.getCacheConfiguration("transactionalWithDisabledInvocationBatching");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.transaction().transactionMode().isTransactional());
      assertEquals("TRANSACTIONAL", config.transaction().transactionMode().name());
      assertTrue(config.transaction().useSynchronization());
      assertFalse(config.transaction().recovery().enabled());
      assertTrue(config.transaction().autoCommit());
      assertFalse(config.invocationBatching().enabled());

      config = cm.getCacheConfiguration("transactional");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.transaction().transactionMode().isTransactional());
      assertEquals("TRANSACTIONAL", config.transaction().transactionMode().name());
      assertFalse(config.transaction().useSynchronization());
      assertTrue(config.transaction().autoCommit());
      assertFalse(config.invocationBatching().enabled());
      assertTrue(config.transaction().recovery().enabled());
      assertEquals("transactional2", config.transaction().recovery().recoveryInfoCacheName());

      config = cm.getCacheConfiguration("transactional2");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.transaction().transactionMode().isTransactional());
      assertEquals("TRANSACTIONAL", config.transaction().transactionMode().name());
      assertFalse(config.transaction().useSynchronization());
      assertFalse(config.transaction().recovery().enabled());
      assertFalse(config.transaction().autoCommit());
      assertFalse(config.invocationBatching().enabled());
      assertEquals("PESSIMISTIC", config.transaction().lockingMode().name());
      assertEquals(10000, config.transaction().cacheStopTimeout());
      assertTrue(config.transaction().transactionManagerLookup() instanceof org.infinispan.test.tx.TestLookup);

      config = cm.getCacheConfiguration("transactional3");
      assertTrue(config.clustering().cacheMode().isReplicated());
      assertTrue(config.transaction().transactionMode().isTransactional());
      assertEquals("TRANSACTIONAL", config.transaction().transactionMode().name());
      assertFalse(config.transaction().useSynchronization());
      assertTrue(config.transaction().autoCommit());
      assertFalse(config.invocationBatching().enabled());
      assertEquals("OPTIMISTIC", config.transaction().lockingMode().name());
      assertEquals("TOTAL_ORDER", config.transaction().transactionProtocol().name());
      assertFalse(config.transaction().recovery().enabled());

      config = cm.getCacheConfiguration("txSyncRepl");
      assertTrue(config.clustering().cacheMode().isReplicated());
      assertTrue(config.transaction().transactionMode().isTransactional());
      assertEquals("TRANSACTIONAL", config.transaction().transactionMode().name());
      assertFalse(config.transaction().useSynchronization());
      assertTrue(config.transaction().autoCommit());
      assertFalse(config.invocationBatching().enabled());
      assertEquals("OPTIMISTIC", config.transaction().lockingMode().name());
      assertFalse(config.transaction().recovery().enabled());
      assertTrue(config.transaction().transactionManagerLookup() instanceof org.infinispan.transaction.lookup.GenericTransactionManagerLookup);
   }

   private void assertCompatibilityConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("withCompatibilityEnabled");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.compatibility().enabled());
      assertTrue(config.compatibility().marshaller() instanceof GenericJBossMarshaller);

      config = cm.getCacheConfiguration("withoutCompatibility");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.compatibility().enabled());
   }

   private void assertBackupsConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("withSitesEnabled");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.sites().backupFor().isBackupFor("test1", "test"));
      assertTrue(config.sites().hasInUseBackup("backupTest"));
      assertTrue(config.sites().hasEnabledBackups());

      List<BackupConfiguration> backupConfigs = config.sites().allBackups();
      for (BackupConfiguration backupConfig : backupConfigs) {
         if (backupConfig.site().equals("backupTest")) {
            assertTrue(backupConfig.enabled());
            assertFalse(backupConfig.isAsyncBackup());
            assertEquals("SYNC", backupConfig.strategy().name());
            assertEquals(17000, backupConfig.replicationTimeout());
            assertTrue(backupConfig.isTwoPhaseCommit());
            assertEquals(15, backupConfig.takeOffline().afterFailures());
            assertEquals(12000, backupConfig.takeOffline().minTimeToWait());
            assertEquals("IGNORE", backupConfig.backupFailurePolicy().name());
         } else if (backupConfig.site().equals("backupTest1")) {
            assertFalse(backupConfig.enabled());
            assertTrue(backupConfig.isAsyncBackup());
            assertEquals("ASYNC", backupConfig.strategy().name());
            assertEquals(18000, backupConfig.replicationTimeout());
            assertFalse(backupConfig.isTwoPhaseCommit());
            assertEquals("org.infinispan.xsite.CountingCustomFailurePolicy", backupConfig.failurePolicyClass());
            assertEquals(17, backupConfig.takeOffline().afterFailures());
            assertEquals(13000, backupConfig.takeOffline().minTimeToWait());
            assertEquals("CUSTOM", backupConfig.backupFailurePolicy().name());
         }
      }
      assertEquals(BackupFailurePolicy.IGNORE, config.sites().getFailurePolicy("backupTest"));
      assertEquals(BackupFailurePolicy.CUSTOM, config.sites().getFailurePolicy("backupTest1"));

      config = cm.getCacheConfiguration("withEmptyBackups");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.sites().backupFor().isBackupFor("test1", "test"));
      assertFalse(config.sites().hasEnabledBackups());

      assertEquals(0, config.sites().allBackups().size());
   }

   private void assertLockingConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("lockingOverriding");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals("REPEATABLE_READ", config.locking().isolationLevel().name());
      assertEquals(1000, config.locking().concurrencyLevel());
      assertEquals(20000, config.locking().lockAcquisitionTimeout());
      assertTrue(config.locking().useLockStriping());

      config = cm.getCacheConfiguration("lockingWithJDBCLoader");
      assertEquals(1000, config.locking().lockAcquisitionTimeout());
      assertEquals(100, config.locking().concurrencyLevel());
      assertTrue(config.locking().supportsConcurrentUpdates());

      config = cm.getCacheConfiguration("lockingWithStoreAsBinary");
      assertEquals("REPEATABLE_READ", config.locking().isolationLevel().name());
      assertEquals(20000, config.locking().lockAcquisitionTimeout());
      assertEquals(1000, config.locking().concurrencyLevel());
   }

   private void assertExpirationEvictionConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("evictionCache");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals(500, config.expiration().wakeUpInterval());
      assertEquals(1000, config.expiration().maxIdle());
      assertEquals(60000, config.expiration().lifespan());
      assertTrue(config.expiration().reaperEnabled());

      assertEquals(EvictionType.COUNT, config.memory().evictionType());
      assertEquals(StorageType.OBJECT, config.memory().storageType());

      config = cm.getCacheConfiguration("expirationCacheWithEnabledReaper");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals(500, config.expiration().wakeUpInterval());
      assertEquals(1000, config.expiration().maxIdle());
      assertEquals(60000, config.expiration().lifespan());
      assertTrue(config.expiration().reaperEnabled());
   }

   private void assertCustomInterceptorsConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("cacheWithCustomInterceptors");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals(6, config.customInterceptors().interceptors().size());

      assertEquals("FIRST", config.customInterceptors().interceptors().get(0).position().name());
      assertTrue(config.customInterceptors().interceptors().get(0).interceptor() instanceof FooInterceptor);

      assertEquals("LAST", config.customInterceptors().interceptors().get(1).position().name());
      assertTrue(config.customInterceptors().interceptors().get(1).interceptor() instanceof FooInterceptor);

      assertEquals("OTHER_THAN_FIRST_OR_LAST", config.customInterceptors().interceptors().get(2).position().name());
      assertEquals(3, config.customInterceptors().interceptors().get(2).index());
      assertTrue(config.customInterceptors().interceptors().get(2).interceptor() instanceof FooInterceptor);

      assertEquals("OTHER_THAN_FIRST_OR_LAST", config.customInterceptors().interceptors().get(3).position().name());
      assertEquals(FooInterceptor.class, config.customInterceptors().interceptors().get(3).before());
      assertTrue(config.customInterceptors().interceptors().get(3).interceptor() instanceof FooInterceptor);

      assertEquals("OTHER_THAN_FIRST_OR_LAST", config.customInterceptors().interceptors().get(4).position().name());
      assertEquals(FooInterceptor.class, config.customInterceptors().interceptors().get(4).after());
      assertTrue(config.customInterceptors().interceptors().get(4).interceptor() instanceof FooInterceptor);

      assertEquals("FIRST", config.customInterceptors().interceptors().get(5).position().name());
      assertTrue(config.customInterceptors().interceptors().get(0).interceptor() instanceof FooInterceptor);
   }

   private void assertDeadlockDetectionConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("withDeadlockDetection");
      assertFalse(config.jmxStatistics().enabled());
      assertTrue(config.clustering().cacheMode().isDistributed());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertEquals(20000, config.clustering().remoteTimeout());
      assertFalse(config.deadlockDetection().enabled());
      assertEquals(-1, config.deadlockDetection().spinDuration());

      config = cm.getCacheConfiguration("lockingWithJDBCLoader");
      assertFalse(config.deadlockDetection().enabled());
      assertEquals(-1, config.deadlockDetection().spinDuration());

      config = cm.getCacheConfiguration("withDeadlockDetectionDisabled");
      assertFalse(config.jmxStatistics().enabled());
      assertTrue(config.clustering().cacheMode().isDistributed());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertEquals(20000, config.clustering().remoteTimeout());
      assertFalse(config.deadlockDetection().enabled());
   }

   private void assertJmxStatisticsConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("jmxEnabled");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.jmxStatistics().enabled());
   }

   private void assertStoreAsBinaryConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("lockingWithStoreAsBinary");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals(StorageType.BINARY, config.memory().storageType());

      config = cm.getCacheConfiguration("lockingWithStoreAsBinaryDisabled");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals(StorageType.OBJECT, config.memory().storageType());

      config = cm.getCacheConfiguration("withoutStoreAsBinary");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals(StorageType.OBJECT, config.memory().storageType());

      config = cm.getCacheConfiguration("storeKeyValueBinary");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals(StorageType.BINARY, config.memory().storageType());

      config = cm.getCacheConfiguration("lazyDeserializationCache");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertEquals(StorageType.BINARY, config.memory().storageType());
   }

   private void assertClusteringConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("transactional3");
      assertTrue(config.clustering().cacheMode().isReplicated());
      assertTrue(config.clustering().cacheMode().isSynchronous());

      config = cm.getCacheConfiguration("lockingSyncInval");
      assertTrue(config.clustering().cacheMode().isInvalidation());
      assertTrue(config.clustering().cacheMode().isSynchronous());

      config = cm.getCacheConfiguration("lockingAsyncInval");
      assertTrue(config.clustering().cacheMode().isInvalidation());
      assertFalse(config.clustering().cacheMode().isSynchronous());
      assertTrue(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());

      config = cm.getCacheConfiguration("syncRepl");
      assertTrue(config.clustering().cacheMode().isReplicated());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertEquals(15000, config.clustering().sync().replTimeout());
      assertEquals(15000, config.clustering().remoteTimeout());
      assertFalse(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());

      config = cm.getCacheConfiguration("asyncRepl");
      assertTrue(config.clustering().cacheMode().isReplicated());
      assertFalse(config.clustering().cacheMode().isSynchronous());
      assertFalse(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());

      config = cm.getCacheConfiguration("asyncReplQueue");
      assertTrue(config.clustering().cacheMode().isReplicated());
      assertFalse(config.clustering().cacheMode().isSynchronous());
      assertFalse(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());

      config = cm.getCacheConfiguration("txSyncRepl");
      assertTrue(config.clustering().cacheMode().isReplicated());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertEquals(15000, config.clustering().remoteTimeout());
      assertFalse(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());

      config = cm.getCacheConfiguration("dist");
      assertTrue(config.clustering().cacheMode().isDistributed());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertEquals(120000, config.clustering().stateTransfer().timeout());
      assertTrue(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(3, config.clustering().hash().numOwners());
      assertTrue(config.clustering().l1().enabled());
      assertEquals(600000, config.clustering().l1().lifespan());
      assertEquals(1200, config.clustering().l1().cleanupTaskFrequency());

      config = cm.getCacheConfiguration("dist_with_capacity_factors");
      assertTrue(config.clustering().cacheMode().isDistributed());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertEquals(120000, config.clustering().stateTransfer().timeout());
      assertTrue(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(3, config.clustering().hash().numOwners());
      assertEquals(1000, config.clustering().hash().numSegments());
      assertTrue(config.clustering().l1().enabled());
      assertEquals(610000, config.clustering().l1().lifespan());

      config = cm.getCacheConfiguration("groups");
      assertTrue(config.clustering().cacheMode().isDistributed());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertTrue(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());
      assertTrue(config.clustering().hash().groups().enabled());
      assertEquals(1, config.clustering().hash().groups().groupers().size());
      assertTrue(config.clustering().hash().groups().groupers().get(0) instanceof org.infinispan.distribution.groups.KXGrouper);

      config = cm.getCacheConfiguration("groupsDisabled");
      assertTrue(config.clustering().cacheMode().isDistributed());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertTrue(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());
      assertFalse(config.clustering().hash().groups().enabled());

      config = cm.getCacheConfiguration("chunkSize");
      assertTrue(config.clustering().cacheMode().isDistributed());
      assertFalse(config.clustering().cacheMode().isSynchronous());
      assertTrue(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(120000, config.clustering().stateTransfer().timeout());
      assertEquals(1000, config.clustering().stateTransfer().chunkSize());
      assertEquals(3, config.clustering().hash().numOwners());
      assertTrue(config.clustering().l1().enabled());
      assertEquals(600000, config.clustering().l1().lifespan());
      assertEquals(1200, config.clustering().l1().cleanupTaskFrequency());

      config = cm.getCacheConfiguration("distAsync");
      assertTrue(config.clustering().cacheMode().isDistributed());
      assertFalse(config.clustering().cacheMode().isSynchronous());
      assertEquals(120000, config.clustering().stateTransfer().timeout());
      assertTrue(config.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(config.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(3, config.clustering().hash().numOwners());
      assertTrue(config.clustering().l1().enabled());
      assertEquals(600000, config.clustering().l1().lifespan());
      assertEquals(1200, config.clustering().l1().cleanupTaskFrequency());

      config = cm.getCacheConfiguration("localCache");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.clustering().cacheMode().isSynchronous());

      config = cm.getCacheConfiguration("hashWithFactory");
      assertTrue(config.clustering().cacheMode().isDistributed());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertTrue(config.clustering().hash().consistentHashFactory() instanceof org.infinispan.distribution.ch.impl.ReplicatedConsistentHashFactory);
      assertFalse(config.clustering().l1().enabled());
   }

   private void assertPersistenceConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("withClusterLoader");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof ClusterLoaderConfiguration);

      ClusterLoaderConfiguration clusterLoaderConfiguration = (ClusterLoaderConfiguration) config.persistence().stores().get(0);
      assertEquals(15000, clusterLoaderConfiguration.remoteCallTimeout());
      assertTrue(clusterLoaderConfiguration.preload());
      assertTrue(clusterLoaderConfiguration.fetchPersistentState());
      assertTrue(clusterLoaderConfiguration.ignoreModifications());
      assertTrue(clusterLoaderConfiguration.purgeOnStartup());
      assertTrue(clusterLoaderConfiguration.shared());

//-----------------------------------------------------------------------------------------

      config = cm.getCacheConfiguration("withFileStore");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertTrue(config.persistence().usingAsyncStore());
      assertFalse(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof SingleFileStoreConfiguration);

      SingleFileStoreConfiguration singleFileStoreConfiguration = (SingleFileStoreConfiguration) config.persistence().stores().get(0);
      assertEquals("/tmp/FileCacheStore-Location", singleFileStoreConfiguration.location());
      assertEquals(5000, singleFileStoreConfiguration.maxEntries());
      assertTrue(singleFileStoreConfiguration.preload());
      assertTrue(singleFileStoreConfiguration.fetchPersistentState());
      assertTrue(singleFileStoreConfiguration.ignoreModifications());
      assertTrue(singleFileStoreConfiguration.purgeOnStartup());
      assertTrue(singleFileStoreConfiguration.async().enabled());
      assertEquals(5, singleFileStoreConfiguration.async().threadPoolSize());
      assertEquals(700, singleFileStoreConfiguration.async().modificationQueueSize());
      assertFalse(singleFileStoreConfiguration.singletonStore().enabled());

//-----------------------------------------------------------------------------------------

      config = cm.getCacheConfiguration("withFileStoreDisabledAsync");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertFalse(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof SingleFileStoreConfiguration);

      singleFileStoreConfiguration = (SingleFileStoreConfiguration) config.persistence().stores().get(0);
      assertEquals("/tmp/FileCacheStore-Location", singleFileStoreConfiguration.location());
      assertEquals(5000, singleFileStoreConfiguration.maxEntries());
      assertTrue(singleFileStoreConfiguration.preload());
      assertTrue(singleFileStoreConfiguration.fetchPersistentState());
      assertTrue(singleFileStoreConfiguration.ignoreModifications());
      assertTrue(singleFileStoreConfiguration.purgeOnStartup());
      assertFalse(singleFileStoreConfiguration.async().enabled());
      assertFalse(singleFileStoreConfiguration.singletonStore().enabled());

//--------------------------------------------------------------------------------------------

      config = cm.getCacheConfiguration("withLoaderDefaults");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertFalse(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof SingleFileStoreConfiguration);

      singleFileStoreConfiguration = (SingleFileStoreConfiguration) config.persistence().stores().get(0);
      assertEquals("/tmp/Another-FileCacheStore-Location", singleFileStoreConfiguration.location());
      assertTrue(singleFileStoreConfiguration.preload());
      assertTrue(singleFileStoreConfiguration.fetchPersistentState());
      assertTrue(singleFileStoreConfiguration.ignoreModifications());
      assertTrue(singleFileStoreConfiguration.purgeOnStartup());
      assertFalse(singleFileStoreConfiguration.shared());

      //--------------------------------------------------------------------------------------------

      config = cm.getCacheConfiguration("withClusterLoader1");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertFalse(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof ClusterLoaderConfiguration);

      clusterLoaderConfiguration = (ClusterLoaderConfiguration) config.persistence().stores().get(0);
      assertEquals(15000, clusterLoaderConfiguration.remoteCallTimeout());
      assertTrue(clusterLoaderConfiguration.preload());
      assertTrue(clusterLoaderConfiguration.fetchPersistentState());
      assertTrue(clusterLoaderConfiguration.ignoreModifications());
      assertTrue(clusterLoaderConfiguration.purgeOnStartup());
      assertTrue(clusterLoaderConfiguration.shared());

      //--------------------------------------------------------------------------------------------
      config = cm.getCacheConfiguration("lockingWithJDBCLoader");
      assertTrue(config.clustering().cacheMode().isClustered());
      assertTrue(config.clustering().cacheMode().isSynchronous());
      assertEquals(20000, config.clustering().sync().replTimeout());
      assertFalse(config.persistence().usingAsyncStore());
      assertTrue(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof SingleFileStoreConfiguration);

      singleFileStoreConfiguration = (SingleFileStoreConfiguration) config.persistence().stores().get(0);
      assertTrue(singleFileStoreConfiguration.preload());
      assertTrue(singleFileStoreConfiguration.fetchPersistentState());
      assertTrue(singleFileStoreConfiguration.ignoreModifications());
      assertTrue(singleFileStoreConfiguration.purgeOnStartup());
      assertFalse(singleFileStoreConfiguration.shared());
      assertFalse(singleFileStoreConfiguration.async().enabled());
      assertTrue(singleFileStoreConfiguration.singletonStore().enabled());
      assertEquals("${java.io.tmpdir}", singleFileStoreConfiguration.location());

      //--------------------------------------------------------------------------------------------
      config = cm.getCacheConfiguration("jdbcStringBasedWithConnectionPool");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertFalse(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof JdbcStringBasedStoreConfiguration);

      JdbcStringBasedStoreConfiguration jdbcStringBasedStoreConfiguration = (JdbcStringBasedStoreConfiguration) config.persistence().stores().get(0);
      assertFalse(jdbcStringBasedStoreConfiguration.fetchPersistentState());
      assertFalse(jdbcStringBasedStoreConfiguration.ignoreModifications());
      assertFalse(jdbcStringBasedStoreConfiguration.purgeOnStartup());
      assertEquals("org.infinispan.persistence.jdbc.configuration.DummyKey2StringMapper", jdbcStringBasedStoreConfiguration.key2StringMapper());
      assertTrue(jdbcStringBasedStoreConfiguration.table().dropOnExit());
      assertTrue(jdbcStringBasedStoreConfiguration.table().createOnStart());
      assertEquals("ISPN_STRING_TABLE", jdbcStringBasedStoreConfiguration.table().tableNamePrefix());
      assertEquals("ID_COLUMN", jdbcStringBasedStoreConfiguration.table().idColumnName());
      assertEquals("VARCHAR(255)", jdbcStringBasedStoreConfiguration.table().idColumnType());
      assertEquals("DATA_COLUMN", jdbcStringBasedStoreConfiguration.table().dataColumnName());
      assertEquals("BINARY", jdbcStringBasedStoreConfiguration.table().dataColumnType());
      assertEquals("TIMESTAMP_COLUMN", jdbcStringBasedStoreConfiguration.table().timestampColumnName());
      assertEquals("BIGINT", jdbcStringBasedStoreConfiguration.table().timestampColumnType());

      PooledConnectionFactoryConfiguration connectionPool = (PooledConnectionFactoryConfiguration) jdbcStringBasedStoreConfiguration.connectionFactory();
      assertEquals("jdbc:h2:mem:infinispan_string_based;DB_CLOSE_DELAY=-1", connectionPool.connectionUrl());
      assertEquals("sa", connectionPool.username());
      assertEquals("sa", connectionPool.password());
      assertEquals("org.h2.Driver", connectionPool.driverClass());

      //----------------------------------------------------------------------------------------------
      config = cm.getCacheConfiguration("jdbcStringBasedWithDataSource");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertFalse(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof JdbcStringBasedStoreConfiguration);

      jdbcStringBasedStoreConfiguration = (JdbcStringBasedStoreConfiguration) config.persistence().stores().get(0);
      assertFalse(jdbcStringBasedStoreConfiguration.fetchPersistentState());
      assertTrue(jdbcStringBasedStoreConfiguration.ignoreModifications());
      assertTrue(jdbcStringBasedStoreConfiguration.purgeOnStartup());
      assertEquals("org.infinispan.persistence.jdbc.configuration.DummyKey2StringMapper", jdbcStringBasedStoreConfiguration.key2StringMapper());
      assertTrue(jdbcStringBasedStoreConfiguration.table().dropOnExit());
      assertTrue(jdbcStringBasedStoreConfiguration.table().createOnStart());
      assertEquals(50, jdbcStringBasedStoreConfiguration.table().batchSize());
      assertEquals(70, jdbcStringBasedStoreConfiguration.table().fetchSize());
      assertEquals("ISPN_STRING_TABLE", jdbcStringBasedStoreConfiguration.table().tableNamePrefix());
      assertEquals("ID_COLUMN", jdbcStringBasedStoreConfiguration.table().idColumnName());
      assertEquals("VARCHAR(255)", jdbcStringBasedStoreConfiguration.table().idColumnType());
      assertEquals("DATA_COLUMN", jdbcStringBasedStoreConfiguration.table().dataColumnName());
      assertEquals("BINARY", jdbcStringBasedStoreConfiguration.table().dataColumnType());
      assertEquals("TIMESTAMP_COLUMN", jdbcStringBasedStoreConfiguration.table().timestampColumnName());
      assertEquals("BIGINT", jdbcStringBasedStoreConfiguration.table().timestampColumnType());

      ManagedConnectionFactoryConfiguration managedConnectionFactoryConfiguration = (ManagedConnectionFactoryConfiguration) jdbcStringBasedStoreConfiguration.connectionFactory();
      assertEquals("url", managedConnectionFactoryConfiguration.jndiUrl());

      //----------------------------------------------------------------------------------------------
      config = cm.getCacheConfiguration("jdbcStringBasedWithSimpleConnection");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertFalse(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof JdbcStringBasedStoreConfiguration);

      jdbcStringBasedStoreConfiguration = (JdbcStringBasedStoreConfiguration) config.persistence().stores().get(0);
      assertTrue(jdbcStringBasedStoreConfiguration.fetchPersistentState());
      assertTrue(jdbcStringBasedStoreConfiguration.ignoreModifications());
      assertFalse(jdbcStringBasedStoreConfiguration.purgeOnStartup());
      assertEquals("org.infinispan.persistence.jdbc.configuration.DummyKey2StringMapper", jdbcStringBasedStoreConfiguration.key2StringMapper());
      assertFalse(jdbcStringBasedStoreConfiguration.table().dropOnExit());
      assertFalse(jdbcStringBasedStoreConfiguration.table().createOnStart());
      assertEquals("ISPN_STRING_TABLE", jdbcStringBasedStoreConfiguration.table().tableNamePrefix());
      assertEquals("ID_COLUMN", jdbcStringBasedStoreConfiguration.table().idColumnName());
      assertEquals("VARCHAR(255)", jdbcStringBasedStoreConfiguration.table().idColumnType());
      assertEquals("DATA_COLUMN", jdbcStringBasedStoreConfiguration.table().dataColumnName());
      assertEquals("BINARY", jdbcStringBasedStoreConfiguration.table().dataColumnType());
      assertEquals("TIMESTAMP_COLUMN", jdbcStringBasedStoreConfiguration.table().timestampColumnName());
      assertEquals("BIGINT", jdbcStringBasedStoreConfiguration.table().timestampColumnType());

      SimpleConnectionFactoryConfiguration simpleConnectionFactoryConfiguration= (SimpleConnectionFactoryConfiguration) jdbcStringBasedStoreConfiguration.connectionFactory();
      assertEquals("jdbc:h2:mem:infinispan_string_based;DB_CLOSE_DELAY=-1", simpleConnectionFactoryConfiguration.connectionUrl());
      assertEquals("sa", simpleConnectionFactoryConfiguration.username());
      assertEquals("sa", simpleConnectionFactoryConfiguration.password());
      assertEquals("org.h2.Driver", simpleConnectionFactoryConfiguration.driverClass());
      assertTrue(jdbcStringBasedStoreConfiguration.singletonStore().enabled());
      assertEquals("testValue", jdbcStringBasedStoreConfiguration.properties().getProperty("testName"));

      //----------------------------------------------------------------------------------------------
      config = cm.getCacheConfiguration("withRemoteStore");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertFalse(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof RemoteStoreConfiguration);

      RemoteStoreConfiguration remoteStoreConfiguration= (RemoteStoreConfiguration) config.persistence().stores().get(0);
      assertFalse(remoteStoreConfiguration.fetchPersistentState());
      assertTrue(remoteStoreConfiguration.shared());
      assertFalse(remoteStoreConfiguration.preload());
      assertFalse(remoteStoreConfiguration.ignoreModifications());
      assertFalse(remoteStoreConfiguration.purgeOnStartup());
      assertTrue(remoteStoreConfiguration.tcpNoDelay());
      assertEquals("org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy", remoteStoreConfiguration.balancingStrategy());
      assertEquals("org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory", remoteStoreConfiguration.transportFactory());
      assertEquals(32, remoteStoreConfiguration.keySizeEstimate());
      assertEquals(512, remoteStoreConfiguration.valueSizeEstimate());
      assertFalse(remoteStoreConfiguration.forceReturnValues());
      assertEquals(5000, remoteStoreConfiguration.connectionTimeout());
      assertFalse(remoteStoreConfiguration.hotRodWrapping());
      assertEquals("org.infinispan.commons.marshall.jboss.GenericJBossMarshaller", remoteStoreConfiguration.marshaller());
      assertEquals("1.0", remoteStoreConfiguration.protocolVersion());
      assertTrue(remoteStoreConfiguration.rawValues());
      assertEquals("test", remoteStoreConfiguration.remoteCacheName());
      assertEquals(12500, remoteStoreConfiguration.socketTimeout());

      assertEquals(1, remoteStoreConfiguration.servers().size());
      assertEquals("127.0.0.1", remoteStoreConfiguration.servers().get(0).host());
      assertEquals(19711, remoteStoreConfiguration.servers().get(0).port());

      assertTrue(remoteStoreConfiguration.asyncExecutorFactory().factory() instanceof org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory);
      assertEquals(1, remoteStoreConfiguration.asyncExecutorFactory().properties().getIntProperty("pool_size", 0));
      assertEquals(10000, remoteStoreConfiguration.asyncExecutorFactory().properties().getIntProperty("queue_size", 0));

      ConnectionPoolConfiguration remoteConnectionPoolConfiguration = remoteStoreConfiguration.connectionPool();
      assertEquals(99, remoteConnectionPoolConfiguration.maxActive());
      assertEquals(97, remoteConnectionPoolConfiguration.maxIdle());
      assertEquals(27, remoteConnectionPoolConfiguration.minIdle());
      assertEquals(98, remoteConnectionPoolConfiguration.maxTotal());
      assertEquals("CREATE_NEW", remoteConnectionPoolConfiguration.exhaustedAction().name());
      assertEquals(50, remoteConnectionPoolConfiguration.minEvictableIdleTime());
      assertEquals(60000, remoteConnectionPoolConfiguration.timeBetweenEvictionRuns());
      assertFalse(remoteConnectionPoolConfiguration.testWhileIdle());

      //-------------------------------------------------------------------------------------------
      config = cm.getCacheConfiguration("withRestStore");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertFalse(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof RestStoreConfiguration);

      RestStoreConfiguration restStoreConfiguration = (RestStoreConfiguration) config.persistence().stores().get(0);
      assertFalse(restStoreConfiguration.fetchPersistentState());
      assertFalse(restStoreConfiguration.ignoreModifications());
      assertTrue(restStoreConfiguration.purgeOnStartup());
      assertTrue(restStoreConfiguration.shared());
      assertFalse(restStoreConfiguration.preload());
      assertEquals("org.infinispan.persistence.keymappers.WrappedByteArrayOrPrimitiveMapper", restStoreConfiguration.key2StringMapper());
      assertEquals("/rest/___defaultcache/", restStoreConfiguration.path());
      assertEquals("localhost", restStoreConfiguration.host());
      assertEquals(18212, restStoreConfiguration.port());
      assertTrue(restStoreConfiguration.appendCacheNameToPath());

      org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration restConnectionPool = restStoreConfiguration.connectionPool();
      assertEquals(10000, restConnectionPool.connectionTimeout());
      assertEquals(10, restConnectionPool.maxConnectionsPerHost());
      assertEquals(10, restConnectionPool.maxTotalConnections());
      assertEquals(20000, restConnectionPool.bufferSize());
      assertEquals(10000, restConnectionPool.socketTimeout());
      assertTrue(restConnectionPool.tcpNoDelay());

      //-------------------------------------------------------------------------------------------
      config = cm.getCacheConfiguration("withLevelDBStore");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertTrue(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof RocksDBStoreConfiguration);

      RocksDBStoreConfiguration rocksDBStoreConfiguration = (RocksDBStoreConfiguration) config.persistence().stores().get(0);
      assertEquals("/tmp/leveldb/data", rocksDBStoreConfiguration.location());
      assertEquals("/tmp/leveldb/expired", rocksDBStoreConfiguration.expiredLocation());
      assertFalse(rocksDBStoreConfiguration.shared());
      assertTrue(rocksDBStoreConfiguration.preload());
      assertEquals(20, rocksDBStoreConfiguration.clearThreshold());
      assertEquals(30, rocksDBStoreConfiguration.expiryQueueSize());
      assertEquals(10, rocksDBStoreConfiguration.blockSize().intValue());
      assertEquals(50,rocksDBStoreConfiguration.cacheSize().longValue());
      assertEquals("SNAPPY", rocksDBStoreConfiguration.compressionType().name());

      //-------------------------------------------------------------------------------------------
      config = cm.getCacheConfiguration("withJpaStore");
      assertFalse(config.clustering().cacheMode().isClustered());
      assertFalse(config.persistence().usingAsyncStore());
      assertTrue(config.persistence().passivation());
      assertTrue(config.persistence().usingStores());
      assertEquals(1, config.persistence().stores().size());
      assertTrue(config.persistence().stores().get(0) instanceof JpaStoreConfiguration);

      JpaStoreConfiguration jpaStoreConfiguration= (JpaStoreConfiguration) config.persistence().stores().get(0);
      assertEquals("TestPersistentName", jpaStoreConfiguration.persistenceUnitName());
      assertEquals(80, jpaStoreConfiguration.batchSize());
      assertEquals("org.infinispan.tools.customs.CustomDataContainer", jpaStoreConfiguration.entityClass().getCanonicalName());
      assertFalse(jpaStoreConfiguration.storeMetadata());
   }

   private void assertUnsafeConverted(EmbeddedCacheManager cm) {
      Configuration config = cm.getCacheConfiguration("withUnsafe");
      assertTrue(config.unsafe().unreliableReturnValues());

      config = cm.getCacheConfiguration("withUnsafeDisabled");
      assertFalse(config.unsafe().unreliableReturnValues());
   }
}
