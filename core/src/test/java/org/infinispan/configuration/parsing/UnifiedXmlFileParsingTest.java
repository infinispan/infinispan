package org.infinispan.configuration.parsing;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.executors.CachedThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ScheduledThreadPoolExecutorFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.EncodingConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.jmx.CustomMBeanServerPropertiesTest;
import org.infinispan.marshall.AdvancedExternalizerTest;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.parsing.UnifiedXmlFileParsingTest")
public class UnifiedXmlFileParsingTest extends AbstractInfinispanTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() throws Exception {
      URL configDir = Thread.currentThread().getContextClassLoader().getResource("configs/unified");
      List<Path> paths = Files.list(Paths.get(configDir.toURI())).collect(Collectors.toList());
      Object[][] configurationFiles = new Object[paths.size()][];
      boolean hasCurrentSchema = false;
      for (int i = 0; i < paths.size(); i++) {
         if (paths.get(i).getFileName().toString().equals(Version.getSchemaVersion() + ".xml")) {
            hasCurrentSchema = true;
         }
         configurationFiles[i] = new Object[]{paths.get(i)};
      }
      // Ensure that we contain the current schema version at the very least
      assertTrue("Could not find a '" + Version.getSchemaVersion() + ".xml' configuration file", hasCurrentSchema);

      return configurationFiles;
   }

   @Test(dataProvider = "configurationFiles")
   public void testParseAndConstructUnifiedXmlFile(Path config) throws IOException {
      String[] parts = config.getFileName().toString().split("\\.");
      int major = Integer.parseInt(parts[0]);
      int minor = Integer.parseInt(parts[1]);

      Properties properties = new Properties();
      properties.put("jboss.server.temp.dir", System.getProperty("java.io.tmpdir"));

      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), false, properties);
      URL url = FileLookupFactory.newInstance().lookupFileLocation(config.toString(), Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parse(url);
      for (ParserVersionCheck check : ParserVersionCheck.values()) {
         if (check.isIncludedBy(major, minor)) {
            check.check(holder);
         }
      }
   }

   public enum ParserVersionCheck {
      INFINISPAN_110(11, 0) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            DefaultThreadFactory threadFactory;
            BlockingThreadPoolExecutorFactory threadPool;

            threadFactory = getGlobalConfiguration(holder).nonBlockingThreadPool().threadFactory();
            assertEquals("infinispan", threadFactory.threadGroup().getName());
            assertEquals("%G %i", threadFactory.threadNamePattern());
            assertEquals(5, threadFactory.initialPriority());
            threadPool = getGlobalConfiguration(holder).nonBlockingThreadPool().threadPoolFactory();
            assertEquals(12, threadPool.coreThreads());
            assertEquals(15, threadPool.maxThreads());
            assertEquals(132, threadPool.queueLength());
            assertEquals(9851, threadPool.keepAlive());

            threadFactory = getGlobalConfiguration(holder).blockingThreadPool().threadFactory();
            assertEquals("infinispan", threadFactory.threadGroup().getName());
            assertEquals("%G %i", threadFactory.threadNamePattern());
            assertEquals(5, threadFactory.initialPriority());
            threadPool = getGlobalConfiguration(holder).blockingThreadPool().threadPoolFactory();
            assertEquals(3, threadPool.coreThreads());
            assertEquals(8, threadPool.maxThreads());
            assertEquals(121, threadPool.queueLength());
            assertEquals(9859, threadPool.keepAlive());

            Configuration indexed = getConfiguration(holder, "indexed");
            assertFalse(indexed.indexing().autoConfig());
            assertTrue(indexed.indexing().enabled());

            Configuration minimalOffHeap = getConfiguration(holder, "minimal-offheap");
            assertEquals(StorageType.OFF_HEAP, minimalOffHeap.memory().storageType());
         }
      },
      INFINISPAN_100(10, 0) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            // tcp-test and mytcp should be identical aside from MERGE3.max_interval
            ProtocolStackConfigurator tcp = holder.getJGroupsStack("tcp-test");
            ProtocolStackConfigurator mytcp = holder.getJGroupsStack("mytcp");
            assertEquals(tcp.getProtocolStack().size(), mytcp.getProtocolStack().size());
            for (int i = 0; i < tcp.getProtocolStack().size(); i++) {
               ProtocolConfiguration proto1 = tcp.getProtocolStack().get(i);
               ProtocolConfiguration proto2 = mytcp.getProtocolStack().get(i);
               assertEquals(proto1.getProtocolName(), proto2.getProtocolName());
               if (proto1.getProtocolName().equals("FD_ALL")) {
                  assertEquals("tcp>FD_ALL>timeout", "3000", proto1.getProperties().get("timeout"));
                  assertEquals("tcp>FD_ALL>interval", "1000", proto1.getProperties().get("interval"));
                  assertEquals("mytcp>FD_ALL>timeout", "3500", proto2.getProperties().get("timeout"));
                  assertEquals("mytcp>FD_ALL>interval", "1000", proto2.getProperties().get("interval"));
               } else {
                  assertEquals(proto1.getProtocolName(), proto1.getProperties(), proto2.getProperties());
               }
            }
            // tcp and tcpgossip should differ only in the PING protocol
            ProtocolStackConfigurator tcpgossip = holder.getJGroupsStack("tcpgossip");
            assertEquals(tcp.getProtocolStack().size(), tcpgossip.getProtocolStack().size());
            for (int i = 0; i < tcp.getProtocolStack().size(); i++) {
               ProtocolConfiguration proto1 = tcp.getProtocolStack().get(i);
               ProtocolConfiguration proto2 = tcpgossip.getProtocolStack().get(i);
               if (proto1.getProtocolName().equals("MPING")) {
                  assertEquals("TCPGOSSIP", proto2.getProtocolName());
               } else {
                  assertEquals(proto1.getProtocolName(), proto2.getProtocolName());
                  assertEquals(proto1.getProtocolName(), proto1.getProperties(), proto2.getProperties());
               }
            }
         }
      },

      INFINISPAN_93(9, 3) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            Configuration local = getConfiguration(holder, "local");
            PersistenceConfiguration persistenceConfiguration = local.persistence();
            assertEquals(5, persistenceConfiguration.connectionAttempts());
            assertEquals(100, persistenceConfiguration.connectionInterval());
            assertEquals(2000, persistenceConfiguration.availabilityInterval());
            assertFalse(persistenceConfiguration.stores().isEmpty());
            AsyncStoreConfiguration asyncConfig = persistenceConfiguration.stores().iterator().next().async();
            assertTrue(asyncConfig.failSilently());
         }
      },

      INFINISPAN_92(9, 2) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            GlobalStateConfiguration gs = getGlobalConfiguration(holder).globalState();
            assertEquals(ConfigurationStorage.OVERLAY, gs.configurationStorage());
            assertEquals(System.getProperty("java.io.tmpdir") + File.separator + "sharedPath", gs.sharedPersistentLocation());

            EncodingConfiguration encoding = getConfiguration(holder, "local").encoding();
            assertEquals(MediaType.APPLICATION_OBJECT, encoding.keyDataType().mediaType());
            assertEquals(MediaType.APPLICATION_OBJECT, encoding.valueDataType().mediaType());

            MemoryConfiguration memory = getConfiguration(holder, "dist-template").memory();
            assertEquals(EvictionStrategy.REMOVE, memory.evictionStrategy());
         }
      },

      INFINISPAN_91(9, 1) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            PartitionHandlingConfiguration ph = getConfiguration(holder, "dist").clustering().partitionHandling();
            assertTrue(ph.enabled());
            assertEquals(PartitionHandling.ALLOW_READS, ph.whenSplit());
            assertEquals(MergePolicy.PREFERRED_NON_NULL, ph.mergePolicy());

            ph = getConfiguration(holder, "repl").clustering().partitionHandling();
            assertFalse(ph.enabled());
            assertEquals(PartitionHandling.ALLOW_READ_WRITES, ph.whenSplit());
            assertEquals(MergePolicy.NONE, ph.mergePolicy());
         }
      },

      INFINISPAN_90(9, 0) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            GlobalConfiguration globalConfiguration = getGlobalConfiguration(holder);
            assertEquals(4, globalConfiguration.transport().initialClusterSize());
            assertEquals(30000, globalConfiguration.transport().initialClusterTimeout());

            MemoryConfiguration mc = getConfiguration(holder, "off-heap-memory").memory();
            assertEquals(StorageType.OFF_HEAP, mc.storageType());
            assertEquals(10000000, mc.size());
            assertEquals(EvictionType.MEMORY, mc.evictionType());

            mc = getConfiguration(holder, "binary-memory").memory();
            assertEquals(StorageType.BINARY, mc.storageType());
            assertEquals(1, mc.size());

            mc = getConfiguration(holder, "object-memory").memory();
            assertEquals(StorageType.OBJECT, mc.storageType());
         }
      },

      INFINISPAN_85(8, 5) {
         @Override
         public boolean isIncludedBy(int major, int minor) {
            return (major == this.major && minor >= this.minor);
         }

         @Override
         public void check(ConfigurationBuilderHolder holder) {
            GlobalStateConfiguration gs = getGlobalConfiguration(holder).globalState();
            assertEquals(ConfigurationStorage.OVERLAY, gs.configurationStorage());
            assertEquals(System.getProperty("java.io.tmpdir") + File.separator + "sharedPath", gs.sharedPersistentLocation());

            EncodingConfiguration encoding = getConfiguration(holder, "local").encoding();
            assertEquals(MediaType.APPLICATION_OBJECT, encoding.keyDataType().mediaType());
            assertEquals(MediaType.APPLICATION_OBJECT, encoding.valueDataType().mediaType());

            PartitionHandlingConfiguration ph = getConfiguration(holder, "dist").clustering().partitionHandling();
            assertTrue(ph.enabled());
            assertEquals(PartitionHandling.ALLOW_READS, ph.whenSplit());
            assertEquals(MergePolicy.PREFERRED_NON_NULL, ph.mergePolicy());

            ph = getConfiguration(holder, "repl").clustering().partitionHandling();
            assertFalse(ph.enabled());
            assertEquals(PartitionHandling.ALLOW_READ_WRITES, ph.whenSplit());
            assertEquals(MergePolicy.NONE, ph.mergePolicy());

            MemoryConfiguration mc = getConfiguration(holder, "off-heap-memory").memory();
            assertEquals(StorageType.OFF_HEAP, mc.storageType());
            assertEquals(10000000, mc.size());
            assertEquals(EvictionType.MEMORY, mc.evictionType());

            mc = getConfiguration(holder, "binary-memory").memory();
            assertEquals(StorageType.BINARY, mc.storageType());
            assertEquals(1, mc.size());

            mc = getConfiguration(holder, "object-memory").memory();
            assertEquals(StorageType.OBJECT, mc.storageType());
         }
      },

      INFINISPAN_84(8, 4) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            // Nothing new
         }
      },

      INFINISPAN_83(8, 3) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            // Nothing new
         }
      },

      INFINISPAN_82(8, 2) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            GlobalConfiguration globalConfiguration = getGlobalConfiguration(holder);
            assertEquals(4, globalConfiguration.transport().initialClusterSize());
            assertEquals(30000, globalConfiguration.transport().initialClusterTimeout());
         }
      },

      INFINISPAN_81(8, 1) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            GlobalConfiguration globalConfiguration = getGlobalConfiguration(holder);
            assertTrue(globalConfiguration.globalState().enabled());
            assertEquals(System.getProperty("java.io.tmpdir") + File.separator + "persistentPath", globalConfiguration.globalState().persistentLocation());
            assertEquals(System.getProperty("java.io.tmpdir") + File.separator + "tmpPath", globalConfiguration.globalState().temporaryLocation());
         }
      },

      INFINISPAN_80(8, 0) {
         @Override
         public void check(ConfigurationBuilderHolder holder) {
            Configuration c = holder.getDefaultConfigurationBuilder().build();
            assertFalse(c.memory().evictionType() == EvictionType.MEMORY);
            c = getConfiguration(holder, "invalid");
            assertTrue(c.memory().evictionType() == EvictionType.COUNT);

            DefaultThreadFactory threadFactory;
            BlockingThreadPoolExecutorFactory threadPool;

            threadFactory = getGlobalConfiguration(holder).asyncThreadPool().threadFactory();
            assertNull(threadFactory);
            threadPool = getGlobalConfiguration(holder).asyncThreadPool().threadPoolFactory();
            assertNull(threadPool);

            threadFactory = getGlobalConfiguration(holder).stateTransferThreadPool().threadFactory();
            assertNull(threadFactory);
            threadPool = getGlobalConfiguration(holder).stateTransferThreadPool().threadPoolFactory();
            assertNull(threadPool);

            assertTemplateConfiguration(holder, "local-template");
            assertTemplateConfiguration(holder, "invalidation-template");
            assertTemplateConfiguration(holder, "repl-template");
            assertTemplateConfiguration(holder, "dist-template");

            assertCacheConfiguration(holder, "local-instance");
            assertCacheConfiguration(holder, "invalidation-instance");
            assertCacheConfiguration(holder, "repl-instance");
            assertCacheConfiguration(holder, "dist-instance");

            Configuration localTemplate = getConfiguration(holder, "local-template");
            Configuration localConfiguration = getConfiguration(holder, "local-instance");
            assertEquals(10000, localTemplate.expiration().wakeUpInterval());
            assertEquals(11000, localConfiguration.expiration().wakeUpInterval());
            assertEquals(10, localTemplate.expiration().lifespan());
            assertEquals(10, localConfiguration.expiration().lifespan());

            Configuration replTemplate = getConfiguration(holder, "repl-template");
            Configuration replConfiguration = getConfiguration(holder, "repl-instance");
            assertEquals(31000, replTemplate.locking().lockAcquisitionTimeout());
            assertEquals(32000, replConfiguration.locking().lockAcquisitionTimeout());
            assertEquals(3000, replTemplate.locking().concurrencyLevel());
            assertEquals(3000, replConfiguration.locking().concurrencyLevel());
         }
      },

      INFINISPAN_70(7, 0) {
         public void check(ConfigurationBuilderHolder holder) {
            GlobalConfiguration g = getGlobalConfiguration(holder);
            assertEquals("maximal", g.cacheManagerName());
            assertTrue(g.statistics());
            assertTrue(g.jmx().enabled());
            assertEquals("my-domain", g.jmx().domain());
            assertTrue(g.jmx().mbeanServerLookup() instanceof CustomMBeanServerPropertiesTest.TestLookup);
            assertEquals(1, g.jmx().properties().size());
            assertEquals("value", g.jmx().properties().getProperty("key"));

            // Transport
            assertEquals("maximal-cluster", g.transport().clusterName());
            assertEquals(120000, g.transport().distributedSyncTimeout());

            assertNotNull("udp", holder.getJGroupsStack("udp"));
            assertNotNull("tcp", holder.getJGroupsStack("tcp"));
            assertEquals(holder.getJGroupsStack("tcp"), g.transport().properties().get(JGroupsTransport.CHANNEL_CONFIGURATOR));

            DefaultThreadFactory threadFactory;
            BlockingThreadPoolExecutorFactory threadPool;

            threadFactory = getGlobalConfiguration(holder).listenerThreadPool().threadFactory();
            assertEquals("infinispan", threadFactory.threadGroup().getName());
            assertEquals("%G %i", threadFactory.threadNamePattern());
            assertEquals(5, threadFactory.initialPriority());
            threadPool = getGlobalConfiguration(holder).listenerThreadPool().threadPoolFactory();
            assertEquals(1, threadPool.coreThreads());
            assertEquals(1, threadPool.maxThreads());
            assertEquals(0, threadPool.queueLength());
            assertEquals(0, threadPool.keepAlive());

            assertTrue(getGlobalConfiguration(holder).expirationThreadPool().threadPoolFactory() instanceof ScheduledThreadPoolExecutorFactory);
            threadFactory = getGlobalConfiguration(holder).expirationThreadPool().threadFactory();
            assertEquals("infinispan", threadFactory.threadGroup().getName());
            assertEquals("%G %i", threadFactory.threadNamePattern());
            assertEquals(5, threadFactory.initialPriority());

            threadFactory = getGlobalConfiguration(holder).transport().remoteCommandThreadPool().threadFactory();
            assertEquals("infinispan", threadFactory.threadGroup().getName());
            assertEquals("%G %i", threadFactory.threadNamePattern());
            assertEquals(5, threadFactory.initialPriority());
            assertTrue(getGlobalConfiguration(holder).transport().remoteCommandThreadPool().threadPoolFactory() instanceof CachedThreadPoolExecutorFactory);

            threadFactory = getGlobalConfiguration(holder).transport().transportThreadPool().threadFactory();
            assertNull(threadFactory);
            threadPool = getGlobalConfiguration(holder).transport().transportThreadPool().threadPoolFactory();
            assertNull(threadPool);

            assertTrue(g.serialization().marshaller() instanceof TestObjectStreamMarshaller);
            Map<Integer, AdvancedExternalizer<?>> externalizers = g.serialization().advancedExternalizers();
            AdvancedExternalizer<?> externalizer = externalizers.get(9001);
            assertTrue(externalizer instanceof AdvancedExternalizerTest.IdViaConfigObj.Externalizer);
            externalizer = externalizers.get(9002);
            assertTrue(externalizer instanceof AdvancedExternalizerTest.IdViaAnnotationObj.Externalizer);
            assertEquals(ShutdownHookBehavior.DONT_REGISTER, g.shutdown().hookBehavior());

            // Default cache is "local" named cache
            Configuration c = holder.getDefaultConfigurationBuilder().build();
            assertFalse(c.invocationBatching().enabled());
            assertTrue(c.statistics().enabled());
            assertEquals(CacheMode.LOCAL, c.clustering().cacheMode());
            assertEquals(30000, c.locking().lockAcquisitionTimeout());
            assertEquals(2000, c.locking().concurrencyLevel());
            assertEquals(IsolationLevel.NONE, c.locking().isolationLevel());
            assertTrue(c.locking().useLockStriping());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Full XA
            assertFalse(c.transaction().useSynchronization()); // Full XA
            assertTrue(c.transaction().recovery().enabled()); // Full XA
            assertEquals(LockingMode.OPTIMISTIC, c.transaction().lockingMode());
            assertTrue(c.transaction().transactionManagerLookup() instanceof JBossStandaloneJTAManagerLookup);
            assertEquals(60000, c.transaction().cacheStopTimeout());
            assertEquals(20000, c.memory().size());
            assertEquals(10000, c.expiration().wakeUpInterval());
            assertEquals(10, c.expiration().lifespan());
            assertEquals(10, c.expiration().maxIdle());
            assertFalse(c.persistence().passivation());
            SingleFileStoreConfiguration fileStore = (SingleFileStoreConfiguration) c.persistence().stores().get(0);
            assertFalse(fileStore.fetchPersistentState());
            assertEquals(System.getProperty("java.io.tmpdir") + File.separator + "path", fileStore.location());
            assertFalse(fileStore.purgeOnStartup());
            assertTrue(fileStore.preload());
            assertFalse(fileStore.shared());
            assertEquals(2048, fileStore.async().modificationQueueSize());
            assertEquals(1, fileStore.async().threadPoolSize());
            assertFalse(c.indexing().enabled());

            c = getConfiguration(holder, "invalid");
            assertEquals(CacheMode.INVALIDATION_SYNC, c.clustering().cacheMode());
            assertTrue(c.invocationBatching().enabled());
            assertTrue(c.statistics().enabled());
            assertEquals(30500, c.locking().lockAcquisitionTimeout());
            assertEquals(2500, c.locking().concurrencyLevel());
            assertEquals(IsolationLevel.READ_COMMITTED, c.locking().isolationLevel()); // Converted to READ_COMMITTED by builder
            assertTrue(c.locking().useLockStriping());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(LockingMode.OPTIMISTIC, c.transaction().lockingMode());
            assertEquals(60500, c.transaction().cacheStopTimeout());
            assertEquals(20500, c.memory().size());
            assertEquals(10500, c.expiration().wakeUpInterval());
            assertEquals(11, c.expiration().lifespan());
            assertEquals(11, c.expiration().maxIdle());
            assertFalse(c.indexing().enabled());

            c = getConfiguration(holder, "repl");
            assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
            assertTrue(c.invocationBatching().enabled());
            assertTrue(c.statistics().enabled());
            assertEquals(31000, c.locking().lockAcquisitionTimeout());
            assertEquals(3000, c.locking().concurrencyLevel());
            assertEquals(IsolationLevel.REPEATABLE_READ, c.locking().isolationLevel()); // Converted to REPEATABLE_READ by builder
            assertTrue(c.locking().useLockStriping());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Batching, non XA
            assertTrue(c.transaction().useSynchronization()); // Batching, non XA
            assertFalse(c.transaction().recovery().enabled()); // Batching, non XA
            assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());
            assertEquals(61000, c.transaction().cacheStopTimeout());
            assertEquals(21000, c.memory().size());
            assertEquals(11000, c.expiration().wakeUpInterval());
            assertEquals(12, c.expiration().lifespan());
            assertEquals(12, c.expiration().maxIdle());
            assertFalse(c.clustering().stateTransfer().fetchInMemoryState());
            assertEquals(60000, c.clustering().stateTransfer().timeout());
            assertEquals(10000, c.clustering().stateTransfer().chunkSize());
            ClusterLoaderConfiguration clusterLoader = getStoreConfiguration(c, ClusterLoaderConfiguration.class);
            assertEquals(35000, clusterLoader.remoteCallTimeout());
            assertFalse(clusterLoader.preload());
            assertFalse(c.indexing().enabled());

            c = getConfiguration(holder, "dist");
            assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
            assertFalse(c.invocationBatching().enabled());
            assertEquals(1200000, c.clustering().l1().lifespan());
            assertEquals(4, c.clustering().hash().numOwners());
            assertEquals(35000, c.clustering().remoteTimeout());
            assertEquals(2, c.clustering().hash().numSegments());
            assertTrue(c.clustering().hash().consistentHashFactory() instanceof SyncConsistentHashFactory);
            assertTrue(c.clustering().partitionHandling().enabled());
            assertTrue(c.statistics().enabled());
            assertEquals(31500, c.locking().lockAcquisitionTimeout());
            assertEquals(3500, c.locking().concurrencyLevel());
            assertEquals(IsolationLevel.READ_COMMITTED, c.locking().isolationLevel());
            assertTrue(c.locking().useLockStriping());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Full XA
            assertFalse(c.transaction().useSynchronization()); // Full XA
            assertTrue(c.transaction().recovery().enabled()); // Full XA
            assertEquals(LockingMode.OPTIMISTIC, c.transaction().lockingMode());
            assertEquals(61500, c.transaction().cacheStopTimeout());
            assertEquals(21500, c.memory().size());
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

            c = getConfiguration(holder, "capedwarf-data");
            assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(StorageType.OBJECT, c.memory().storageType());
            assertEquals(-1, c.memory().size());
            fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
            assertTrue(fileStore.preload());
            assertFalse(fileStore.purgeOnStartup());

            c = getConfiguration(holder, "capedwarf-metadata");
            assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(StorageType.OBJECT, c.memory().storageType());
            assertEquals(-1, c.memory().size());
            DummyInMemoryStoreConfiguration dummyStore = getStoreConfiguration(c, DummyInMemoryStoreConfiguration.class);
            assertFalse(dummyStore.preload());
            assertFalse(dummyStore.purgeOnStartup());

            c = getConfiguration(holder, "capedwarf-memcache");
            assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(StorageType.OBJECT, c.memory().storageType());
            assertEquals(-1, c.memory().size());
            assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());

            c = getConfiguration(holder, "capedwarf-default");
            assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(StorageType.OBJECT, c.memory().storageType());
            assertEquals(-1, c.memory().size());
            fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
            assertTrue(fileStore.preload());
            assertFalse(fileStore.purgeOnStartup());
            assertFalse(c.indexing().enabled());

            c = getConfiguration(holder, "capedwarf-dist");
            assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(StorageType.OBJECT, c.memory().storageType());
            assertEquals(-1, c.memory().size());
            assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());
            fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
            assertTrue(fileStore.preload());
            assertFalse(fileStore.purgeOnStartup());

            c = getConfiguration(holder, "capedwarf-tasks");
            assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(10000, c.memory().size());
            fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
            assertTrue(fileStore.preload());
            assertFalse(fileStore.purgeOnStartup());
            assertFalse(c.indexing().enabled());

            c = getConfiguration(holder, "HibernateSearch-LuceneIndexesMetadata");
            assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.invocationBatching().enabled());
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(-1, c.memory().size());
            fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
            assertTrue(fileStore.preload());
            assertFalse(fileStore.purgeOnStartup());

            c = getConfiguration(holder, "HibernateSearch-LuceneIndexesData");
            assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.invocationBatching().enabled());
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(-1, c.memory().size());
            fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
            assertTrue(fileStore.preload());
            assertFalse(fileStore.purgeOnStartup());

            c = getConfiguration(holder, "HibernateSearch-LuceneIndexesLocking");
            assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
            assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
            assertTrue(c.invocationBatching().enabled());
            assertTrue(c.transaction().useSynchronization()); // Non XA
            assertFalse(c.transaction().recovery().enabled()); // Non XA
            assertEquals(-1, c.memory().size());

            c = getConfiguration(holder, "custom-interceptors");
            List<InterceptorConfiguration> interceptors = c.customInterceptors().interceptors();
            InterceptorConfiguration interceptor = interceptors.get(0);
            assertTrue(interceptor.asyncInterceptor() instanceof CustomInterceptor1);
            assertEquals(InvocationContextInterceptor.class, interceptor.after());
            interceptor = interceptors.get(1);
            assertEquals(InvocationContextInterceptor.class, interceptor.before());
            assertTrue(interceptor.asyncInterceptor() instanceof CustomInterceptor2);
            interceptor = interceptors.get(2);
            assertTrue(interceptor.asyncInterceptor() instanceof CustomInterceptor3);
            assertEquals(1, interceptor.index());
            interceptor = interceptors.get(3);
            assertTrue(interceptor.asyncInterceptor() instanceof CustomInterceptor4);
            assertEquals(InterceptorConfiguration.Position.LAST, interceptor.position());
            assertTrue(c.unsafe().unreliableReturnValues());

            c = getConfiguration(holder, "write-skew");
            assertEquals(IsolationLevel.REPEATABLE_READ, c.locking().isolationLevel());

            // Ignore custom-container (if present)

            if (holder.getNamedConfigurationBuilders().containsKey("store-as-binary")) {
               c = getConfiguration(holder, "store-as-binary");
               assertSame(StorageType.BINARY, c.memory().storageType());
            }
         }

      },
      ;

      protected final int major;
      protected final int minor;

      ParserVersionCheck(int major, int minor) {
         this.major = major;
         this.minor = minor;
      }

      public abstract void check(ConfigurationBuilderHolder cm);

      public boolean isIncludedBy(int major, int minor) {
         return major > this.major || (major == this.major && minor >= this.minor);
      }
   }

   private static void assertTemplateConfiguration(ConfigurationBuilderHolder holder, String name) {
      Configuration configuration = getConfiguration(holder, name);
      assertNotNull("Configuration " + name + " expected", configuration);
      assertTrue("Configuration " + name + " should be a template", configuration.isTemplate());
   }

   private static void assertCacheConfiguration(ConfigurationBuilderHolder holder, String name) {
      Configuration configuration = getConfiguration(holder, name);
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

   public static final class CustomInterceptor1 extends BaseCustomAsyncInterceptor {
   }

   public static final class CustomInterceptor2 extends BaseCustomAsyncInterceptor {
   }

   public static final class CustomInterceptor3 extends BaseCustomAsyncInterceptor {
   }

   public static final class CustomInterceptor4 extends BaseCustomAsyncInterceptor {
      String foo; // configured via XML
   }

   private static Configuration getConfiguration(ConfigurationBuilderHolder holder, String name) {
      ConfigurationBuilder builder = holder.getNamedConfigurationBuilders().get(name);
      return Objects.requireNonNull(builder).build();
   }

   private static GlobalConfiguration getGlobalConfiguration(ConfigurationBuilderHolder holder) {
      return holder.getGlobalConfigurationBuilder().build();
   }

}
