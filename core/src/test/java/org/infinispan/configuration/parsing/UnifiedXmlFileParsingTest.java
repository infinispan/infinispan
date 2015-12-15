package org.infinispan.configuration.parsing;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.infinispan.Version;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.executors.CachedThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ScheduledThreadPoolExecutorFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.QueryableDataContainer;
import org.infinispan.configuration.cache.*;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.CustomMBeanServerPropertiesTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.AdvancedExternalizerTest;
import org.infinispan.marshall.core.VersionAwareMarshaller;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.parsing.UnifiedXmlFileParsingTest")
public class UnifiedXmlFileParsingTest extends AbstractInfinispanTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][] { {"7.0.xml"}, {"7.1.xml"}, {"7.2.xml"}, {"8.0.xml"}, {"8.1.xml"}, {"8.2.xml"} };
   }

   @Test(dataProvider="configurationFiles")
   public void testParseAndConstructUnifiedXmlFile(String config) throws IOException {
      String[] parts = config.split("\\.");
      int major = Integer.parseInt(parts[0]);
      int minor = Integer.parseInt(parts[1]);
      final int version = major * 10 + minor;
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.fromXml("configs/unified/" + config, true, false)) {
         @Override
         public void call() {
            switch (version) {
               case 82:
                  configurationCheck82(cm);
                  break;
               case 81:
                  configurationCheck81(cm);
                  break;
               case 80:
                  configurationCheck80(cm);
                  break;
               case 70:
               case 71:
               case 72:
                  configurationCheck70(cm);
                  break;
               default:
                  throw new IllegalArgumentException("Unknown configuration version "+version);
            }
         }
      });
   }

   private static void configurationCheck82(EmbeddedCacheManager cm) {
      configurationCheck81(cm);
   }

   private static void configurationCheck81(EmbeddedCacheManager cm) {
      configurationCheck80(cm);
      GlobalConfiguration globalConfiguration = cm.getCacheManagerConfiguration();
      assertTrue(globalConfiguration.globalState().enabled());
      assertEquals("persistentPath", globalConfiguration.globalState().persistentLocation());
      assertEquals("tmpPath", globalConfiguration.globalState().temporaryLocation());
   }

   private static void configurationCheck80(EmbeddedCacheManager cm) {
      configurationCheck70(cm);
      Configuration c = cm.getCache().getCacheConfiguration();
      assertFalse(c.eviction().type() == EvictionType.MEMORY);
      c = cm.getCache("invalid").getCacheConfiguration();
      assertTrue(c.eviction().type() == EvictionType.MEMORY);

      DefaultThreadFactory threadFactory;
      BlockingThreadPoolExecutorFactory threadPool;

      threadFactory = cm.getCacheManagerConfiguration().asyncThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      threadPool = cm.getCacheManagerConfiguration().asyncThreadPool().threadPoolFactory();
      assertEquals(TestCacheManagerFactory.ASYNC_EXEC_MAX_THREADS, threadPool.coreThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.ASYNC_EXEC_MAX_THREADS, threadPool.maxThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.ASYNC_EXEC_QUEUE_SIZE, threadPool.queueLength()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.KEEP_ALIVE, threadPool.keepAlive());  // overriden by TestCacheManagerFactory

      threadFactory = cm.getCacheManagerConfiguration().stateTransferThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      threadPool = cm.getCacheManagerConfiguration().stateTransferThreadPool().threadPoolFactory();
      assertEquals(TestCacheManagerFactory.STATE_TRANSFER_EXEC_MAX_THREADS, threadPool.coreThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.STATE_TRANSFER_EXEC_MAX_THREADS, threadPool.maxThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.STATE_TRANSFER_EXEC_QUEUE_SIZE, threadPool.queueLength()); //
      // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.KEEP_ALIVE, threadPool.keepAlive());  // overriden by TestCacheManagerFactory

      assertTemplateConfiguration(cm, "local-template");
      assertTemplateConfiguration(cm, "invalidation-template");
      assertTemplateConfiguration(cm, "repl-template");
      assertTemplateConfiguration(cm, "dist-template");

      assertCacheConfiguration(cm, "local-instance");
      assertCacheConfiguration(cm, "invalidation-instance");
      assertCacheConfiguration(cm, "repl-instance");
      assertCacheConfiguration(cm, "dist-instance");

      Configuration replTemplate = cm.getCacheConfiguration("repl-template");
      Configuration replConfiguration = cm.getCacheConfiguration("repl-instance");
      assertEquals(31000, replTemplate.locking().lockAcquisitionTimeout());
      assertEquals(32000, replConfiguration.locking().lockAcquisitionTimeout());
   }

   private static void configurationCheck70(EmbeddedCacheManager cm) {
      GlobalConfiguration g = cm.getCacheManagerConfiguration();
      assertEquals("maximal", g.globalJmxStatistics().cacheManagerName());
      assertTrue(g.globalJmxStatistics().enabled());
      assertEquals("my-domain", g.globalJmxStatistics().domain());
      assertTrue(g.globalJmxStatistics().mbeanServerLookup() instanceof CustomMBeanServerPropertiesTest.TestLookup);
      assertEquals(1, g.globalJmxStatistics().properties().size());
      assertEquals("value", g.globalJmxStatistics().properties().getProperty("key"));

      // Transport
      assertEquals("maximal-cluster", g.transport().clusterName());
      assertEquals(120000, g.transport().distributedSyncTimeout());
      assertEquals("udp", g.transport().properties().getProperty("stack-udp"));
      assertEquals("tcp", g.transport().properties().getProperty("stack-tcp"));
      assertEquals("jgroups-udp.xml", g.transport().properties().getProperty("stackFilePath-udp"));
      assertEquals("jgroups-tcp.xml", g.transport().properties().getProperty("stackFilePath-tcp"));
      assertEquals("tcp", g.transport().properties().getProperty("stack"));

      DefaultThreadFactory threadFactory;
      BlockingThreadPoolExecutorFactory threadPool;

      threadFactory = cm.getCacheManagerConfiguration().listenerThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      threadPool = cm.getCacheManagerConfiguration().listenerThreadPool().threadPoolFactory();
      assertEquals(1, threadPool.coreThreads());
      assertEquals(1, threadPool.maxThreads());
      assertEquals(0, threadPool.queueLength());
      assertEquals(0, threadPool.keepAlive());

      assertTrue(cm.getCacheManagerConfiguration().transport().totalOrderThreadPool().threadPoolFactory() instanceof CachedThreadPoolExecutorFactory);
      threadFactory = cm.getCacheManagerConfiguration().transport().totalOrderThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());

      assertTrue(cm.getCacheManagerConfiguration().evictionThreadPool().threadPoolFactory() instanceof ScheduledThreadPoolExecutorFactory);
      threadFactory = cm.getCacheManagerConfiguration().evictionThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());

      assertTrue(cm.getCacheManagerConfiguration().replicationQueueThreadPool().threadPoolFactory() instanceof ScheduledThreadPoolExecutorFactory);
      threadFactory = cm.getCacheManagerConfiguration().replicationQueueThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());

      assertTrue(cm.getCacheManagerConfiguration().replicationQueueThreadPool().threadPoolFactory() instanceof ScheduledThreadPoolExecutorFactory);
      threadFactory = cm.getCacheManagerConfiguration().replicationQueueThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());

      threadFactory = cm.getCacheManagerConfiguration().transport().remoteCommandThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      threadPool = cm.getCacheManagerConfiguration().transport().remoteCommandThreadPool().threadPoolFactory();
      assertEquals(TestCacheManagerFactory.REMOTE_EXEC_MAX_THREADS, threadPool.coreThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.REMOTE_EXEC_MAX_THREADS, threadPool.maxThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.REMOTE_EXEC_QUEUE_SIZE, threadPool.queueLength()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.KEEP_ALIVE, threadPool.keepAlive());  // overriden by TestCacheManagerFactory

      threadFactory = cm.getCacheManagerConfiguration().transport().transportThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("%G %i", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      threadPool = cm.getCacheManagerConfiguration().transport().transportThreadPool().threadPoolFactory();
      assertEquals(TestCacheManagerFactory.TRANSPORT_EXEC_MAX_THREADS, threadPool.coreThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.TRANSPORT_EXEC_MAX_THREADS, threadPool.maxThreads()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.TRANSPORT_EXEC_QUEUE_SIZE, threadPool.queueLength()); // overriden by TestCacheManagerFactory
      assertEquals(TestCacheManagerFactory.KEEP_ALIVE, threadPool.keepAlive());  // overriden by TestCacheManagerFactory

      assertTrue(g.serialization().marshaller() instanceof VersionAwareMarshaller);
      assertEquals(Version.getVersionShort("1.0"), g.serialization().version());
      Map<Integer, AdvancedExternalizer<?>> externalizers = g.serialization().advancedExternalizers();
      AdvancedExternalizer<?> externalizer = externalizers.get(9001);
      assertTrue(externalizer instanceof AdvancedExternalizerTest.IdViaConfigObj.Externalizer);
      externalizer = externalizers.get(9002);
      assertTrue(externalizer instanceof AdvancedExternalizerTest.IdViaAnnotationObj.Externalizer);
      assertEquals(ShutdownHookBehavior.DONT_REGISTER, g.shutdown().hookBehavior());

      // Default cache is "local" named cache
      Configuration c = cm.getCache().getCacheConfiguration();
      assertFalse(c.invocationBatching().enabled());
      assertTrue(c.jmxStatistics().enabled());
      assertEquals(CacheMode.LOCAL, c.clustering().cacheMode());
      assertEquals(30000, c.locking().lockAcquisitionTimeout());
      assertEquals(2000, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.NONE, c.locking().isolationLevel());
      assertTrue(c.locking().useLockStriping());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Full XA
      assertFalse(c.transaction().useSynchronization()); // Full XA
      assertTrue(c.transaction().recovery().enabled()); // Full XA
      assertTrue(c.transaction().syncCommitPhase()); // Full XA
      assertTrue(c.transaction().syncRollbackPhase()); // Full XA
      assertEquals(LockingMode.OPTIMISTIC, c.transaction().lockingMode());
      assertTrue(c.transaction().transactionManagerLookup() instanceof JBossStandaloneJTAManagerLookup);
      assertEquals(60000, c.transaction().cacheStopTimeout());
      assertEquals(20000, c.eviction().maxEntries());
      assertEquals(EvictionStrategy.LIRS, c.eviction().strategy());
      assertEquals(10000, c.expiration().wakeUpInterval());
      assertEquals(10, c.expiration().lifespan());
      assertEquals(10, c.expiration().maxIdle());
      assertFalse(c.persistence().passivation());
      SingleFileStoreConfiguration fileStore = (SingleFileStoreConfiguration) c.persistence().stores().get(0);
      assertFalse(fileStore.fetchPersistentState());
      assertEquals("path", fileStore.location());
      assertFalse(fileStore.singletonStore().enabled());
      assertFalse(fileStore.purgeOnStartup());
      assertTrue(fileStore.preload());
      assertTrue(fileStore.shared());
      assertEquals(2048, fileStore.async().modificationQueueSize());
      assertEquals(1, fileStore.async().threadPoolSize());
      assertEquals(Index.NONE, c.indexing().index());

      c = cm.getCache("invalid").getCacheConfiguration();
      assertEquals(CacheMode.INVALIDATION_ASYNC, c.clustering().cacheMode());
      assertTrue(c.invocationBatching().enabled());
      assertEquals(10, c.clustering().async().replQueueInterval());
      assertEquals(1000, c.clustering().async().replQueueMaxElements());
      assertTrue(c.jmxStatistics().enabled());
      assertEquals(30500, c.locking().lockAcquisitionTimeout());
      assertEquals(2500, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.READ_COMMITTED, c.locking().isolationLevel()); // Converted to READ_COMMITTED by builder
      assertTrue(c.locking().useLockStriping());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(LockingMode.OPTIMISTIC, c.transaction().lockingMode());
      assertEquals(60500, c.transaction().cacheStopTimeout());
      assertEquals(20500, c.eviction().size());
      assertEquals(EvictionStrategy.LRU, c.eviction().strategy());
      assertEquals(10500, c.expiration().wakeUpInterval());
      assertEquals(11, c.expiration().lifespan());
      assertEquals(11, c.expiration().maxIdle());
      assertEquals(Index.NONE, c.indexing().index());

      c = cm.getCache("repl").getCacheConfiguration();
      assertEquals(CacheMode.REPL_ASYNC, c.clustering().cacheMode());
      assertTrue(c.invocationBatching().enabled());
      assertEquals(11, c.clustering().async().replQueueInterval());
      assertEquals(1500, c.clustering().async().replQueueMaxElements());
      assertFalse(c.clustering().async().asyncMarshalling());
      assertTrue(c.jmxStatistics().enabled());
      assertEquals(31000, c.locking().lockAcquisitionTimeout());
      assertEquals(3000, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.REPEATABLE_READ, c.locking().isolationLevel()); // Converted to REPEATABLE_READ by builder
      assertTrue(c.locking().useLockStriping());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Batching, non XA
      assertTrue(c.transaction().useSynchronization()); // Batching, non XA
      assertFalse(c.transaction().recovery().enabled()); // Batching, non XA
      assertTrue(c.transaction().syncCommitPhase()); // Batching, non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Batching, non XA - side effect of cache manager creation
      assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());
      assertEquals(61000, c.transaction().cacheStopTimeout());
      assertEquals(21000, c.eviction().maxEntries());
      assertEquals(EvictionStrategy.FIFO, c.eviction().strategy());
      assertEquals(11000, c.expiration().wakeUpInterval());
      assertEquals(12, c.expiration().lifespan());
      assertEquals(12, c.expiration().maxIdle());
      assertFalse(c.clustering().stateTransfer().fetchInMemoryState());
      assertEquals(60000, c.clustering().stateTransfer().timeout());
      assertEquals(10000, c.clustering().stateTransfer().chunkSize());
      ClusterLoaderConfiguration clusterLoader = getStoreConfiguration(c, ClusterLoaderConfiguration.class);
      assertEquals(35000, clusterLoader.remoteCallTimeout());
      assertFalse(clusterLoader.preload());
      assertEquals(Index.NONE, c.indexing().index());

      c = cm.getCache("dist").getCacheConfiguration();
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertFalse(c.invocationBatching().enabled());
      assertEquals(1200000, c.clustering().l1().lifespan());
      assertEquals(4, c.clustering().hash().numOwners());
      assertEquals(35000, c.clustering().sync().replTimeout());
      assertEquals(2, c.clustering().hash().numSegments());
      assertTrue(c.clustering().hash().consistentHashFactory() instanceof SyncConsistentHashFactory);
      assertFalse(c.clustering().async().asyncMarshalling());
      assertTrue(c.clustering().partitionHandling().enabled());
      assertTrue(c.jmxStatistics().enabled());
      assertEquals(31500, c.locking().lockAcquisitionTimeout());
      assertEquals(3500, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.READ_COMMITTED, c.locking().isolationLevel());
      assertTrue(c.locking().useLockStriping());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Full XA
      assertFalse(c.transaction().useSynchronization()); // Full XA
      assertTrue(c.transaction().recovery().enabled()); // Full XA
      assertTrue(c.transaction().syncCommitPhase()); // Full XA
      assertTrue(c.transaction().syncRollbackPhase()); // Full XA
      assertEquals(LockingMode.OPTIMISTIC, c.transaction().lockingMode());
      assertEquals(61500, c.transaction().cacheStopTimeout());
      assertEquals(21500, c.eviction().maxEntries());
      assertEquals(EvictionStrategy.UNORDERED, c.eviction().strategy());
      assertEquals(11500, c.expiration().wakeUpInterval());
      assertEquals(13, c.expiration().lifespan());
      assertEquals(13, c.expiration().maxIdle());
      assertTrue(c.clustering().stateTransfer().fetchInMemoryState());
      assertEquals(60500, c.clustering().stateTransfer().timeout());
      assertEquals(10500, c.clustering().stateTransfer().chunkSize());
      // Back up cross-site configuration
      BackupConfiguration backup = c.sites().allBackups().get(0);
      assertEquals("NYC", backup.site());
      assertEquals(BackupFailurePolicy.WARN, backup.backupFailurePolicy());
      assertEquals(BackupConfiguration.BackupStrategy.SYNC, backup.strategy());
      assertEquals(12500, backup.replicationTimeout());
      assertFalse(backup.enabled());
      backup = c.sites().allBackups().get(1);
      assertEquals("SFO", backup.site());
      assertEquals(BackupFailurePolicy.IGNORE, backup.backupFailurePolicy());
      assertEquals(BackupConfiguration.BackupStrategy.ASYNC, backup.strategy());
      assertEquals(13000, backup.replicationTimeout());
      assertTrue(backup.enabled());
      backup = c.sites().allBackups().get(2);
      assertEquals("LON", backup.site());
      assertEquals(BackupFailurePolicy.FAIL, backup.backupFailurePolicy());
      assertEquals(BackupConfiguration.BackupStrategy.SYNC, backup.strategy());
      assertEquals(13500, backup.replicationTimeout());
      assertTrue(backup.enabled());
      assertEquals(3, backup.takeOffline().afterFailures());
      assertEquals(10000, backup.takeOffline().minTimeToWait());
      assertEquals("users", c.sites().backupFor().remoteCache());
      assertEquals("LON", c.sites().backupFor().remoteSite());

      c = cm.getCache("capedwarf-data").getCacheConfiguration();
      assertEquals(CacheMode.REPL_ASYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(EvictionStrategy.NONE, c.eviction().strategy());
      assertEquals(-1, c.eviction().maxEntries());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());

      c = cm.getCache("capedwarf-metadata").getCacheConfiguration();
      assertEquals(CacheMode.REPL_ASYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(EvictionStrategy.NONE, c.eviction().strategy());
      assertEquals(-1, c.eviction().maxEntries());
      DummyInMemoryStoreConfiguration dummyStore = getStoreConfiguration(c, DummyInMemoryStoreConfiguration.class);
      assertFalse(dummyStore.preload());
      assertFalse(dummyStore.purgeOnStartup());

      c = cm.getCache("capedwarf-memcache").getCacheConfiguration();
      assertEquals(CacheMode.REPL_ASYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(EvictionStrategy.NONE, c.eviction().strategy());
      assertEquals(-1, c.eviction().maxEntries());
      assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());

      c = cm.getCache("capedwarf-default").getCacheConfiguration();
      assertEquals(CacheMode.DIST_ASYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(EvictionStrategy.NONE, c.eviction().strategy());
      assertEquals(-1, c.eviction().maxEntries());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());
      assertEquals(Index.NONE, c.indexing().index());

      c = cm.getCache("capedwarf-dist").getCacheConfiguration();
      assertEquals(CacheMode.DIST_ASYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(EvictionStrategy.NONE, c.eviction().strategy());
      assertEquals(-1, c.eviction().maxEntries());
      assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());

      c = cm.getCache("capedwarf-tasks").getCacheConfiguration();
      assertEquals(CacheMode.DIST_ASYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(EvictionStrategy.LIRS, c.eviction().strategy());
      assertEquals(10000, c.eviction().maxEntries());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());
      assertEquals(Index.NONE, c.indexing().index());

      c = cm.getCache("HibernateSearch-LuceneIndexesMetadata").getCacheConfiguration();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.invocationBatching().enabled());
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(EvictionStrategy.NONE, c.eviction().strategy());
      assertEquals(-1, c.eviction().maxEntries());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());

      c = cm.getCache("HibernateSearch-LuceneIndexesData").getCacheConfiguration();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.invocationBatching().enabled());
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(EvictionStrategy.NONE, c.eviction().strategy());
      assertEquals(-1, c.eviction().maxEntries());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());

      c = cm.getCache("HibernateSearch-LuceneIndexesLocking").getCacheConfiguration();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.invocationBatching().enabled());
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertTrue(c.transaction().syncCommitPhase()); // Non XA - default configuration value
      assertTrue(c.transaction().syncRollbackPhase()); // Non XA - side effect of cache manager creation
      assertEquals(EvictionStrategy.NONE, c.eviction().strategy());
      assertEquals(-1, c.eviction().maxEntries());

      c = cm.getCache("custom-interceptors").getCacheConfiguration();
      List<InterceptorConfiguration> interceptors = c.customInterceptors().interceptors();
      InterceptorConfiguration interceptor = interceptors.get(0);
      assertTrue(interceptor.interceptor() instanceof CustomInterceptor1);
      assertEquals(InvocationContextInterceptor.class, interceptor.after());
      interceptor = interceptors.get(1);
      assertEquals(InvocationContextInterceptor.class, interceptor.before());
      assertTrue(interceptor.interceptor() instanceof CustomInterceptor2);
      interceptor = interceptors.get(2);
      assertTrue(interceptor.interceptor() instanceof CustomInterceptor3);
      assertEquals(1, interceptor.index());
      interceptor = interceptors.get(3);
      assertTrue(interceptor.interceptor() instanceof CustomInterceptor4);
      assertEquals(InterceptorConfiguration.Position.LAST, interceptor.position());
      assertTrue(c.unsafe().unreliableReturnValues());

      c = cm.getCache("write-skew").getCacheConfiguration();
      assertTrue(c.locking().writeSkewCheck());
      assertEquals(IsolationLevel.REPEATABLE_READ, c.locking().isolationLevel());
      assertTrue(c.versioning().enabled());
      assertEquals(VersioningScheme.SIMPLE, c.versioning().scheme());
      assertFalse(c.deadlockDetection().enabled());

      c = cm.getCache("compatibility").getCacheConfiguration();
      assertTrue(c.compatibility().enabled());
      assertTrue(c.compatibility().marshaller() instanceof GenericJBossMarshaller);
      assertTrue(c.deadlockDetection().enabled());
      assertEquals(200, c.deadlockDetection().spinDuration());

      c = cm.getCache("custom-container").getCacheConfiguration();
      assertTrue(c.dataContainer().dataContainer() instanceof QueryableDataContainer);
      assertTrue(c.dataContainer().<byte[]>keyEquivalence() instanceof ByteArrayEquivalence);
      assertTrue(c.dataContainer().<byte[]>valueEquivalence() instanceof ByteArrayEquivalence);

      c = cm.getCache("store-as-binary").getCacheConfiguration();
      assertTrue(c.storeAsBinary().enabled());
      assertTrue(c.storeAsBinary().storeKeysAsBinary());
      assertFalse(c.storeAsBinary().storeValuesAsBinary());
   }

   private static void assertTemplateConfiguration(EmbeddedCacheManager cm, String name) {
      Configuration configuration = cm.getCacheConfiguration(name);
      assertNotNull("Configuration " + name + " expected", configuration);
      assertTrue("Configuration " + name + " should be a template", configuration.isTemplate());
   }

   private static void assertCacheConfiguration(EmbeddedCacheManager cm, String name) {
      Configuration configuration = cm.getCacheConfiguration(name);
      assertNotNull("Configuration " + name + " expected", configuration);
      assertFalse("Configuration " + name + " should not be a template", configuration.isTemplate());
   }

   private static <T> T getStoreConfiguration(Configuration c, Class<T> configurationClass) {
      for (StoreConfiguration pc : c.persistence().stores()) {
         if (configurationClass.isInstance(pc)) {
            return (T) pc;
         }
      }
      throw new NoSuchElementException("There is no store of type " + configurationClass);
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testUnsupportedConfiguration() throws Exception {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.fromXml("configs/unified/old.xml", true)) {

               @Override
               public void call() {
                  fail("Parsing an unsupported file should have failed.");
               }

      });
   }

   public static final class CustomInterceptor1 extends CommandInterceptor {}
   public static final class CustomInterceptor2 extends CommandInterceptor {}
   public static final class CustomInterceptor3 extends CommandInterceptor {}
   public static final class CustomInterceptor4 extends CommandInterceptor {
      String foo; // configured via XML
   }

}
