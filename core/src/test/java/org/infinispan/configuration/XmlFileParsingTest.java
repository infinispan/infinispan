package org.infinispan.configuration;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.infinispan.Version;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.interceptors.FooInterceptor;
import org.infinispan.marshall.AdvancedExternalizerTest;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.tx.TestLookup;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   @Test(expectedExceptions=FileNotFoundException.class)
   public void testFailOnUnexpectedConfigurationFile() throws IOException {
      TestCacheManagerFactory.fromXml("does-not-exist.xml");
   }

   public void testNamedCacheFile() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/named-cache-test.xml");
      assertNamedCacheFile(holder, false);
   }

   public void testNoNamedCaches() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <transport cluster=\"demoCluster\"/>\n" +
            "   <replicated-cache name=\"default\">\n" +
            "   </replicated-cache>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            GlobalConfiguration globalCfg = cm.getCacheManagerConfiguration();

            assertTrue(globalCfg.transport().transport() instanceof JGroupsTransport);
            assertEquals("demoCluster", globalCfg.transport().clusterName());

            Configuration cfg = cm.getDefaultCacheConfiguration();
            assertEquals(CacheMode.REPL_SYNC, cfg.clustering().cacheMode());
         }

      });

   }

   @Test(expectedExceptions=CacheConfigurationException.class)
   public void testDuplicateCacheNames() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"duplicatename\">" +
            "   <transport cluster=\"demoCluster\"/>\n" +
            "   <distributed-cache name=\"duplicatename\">\n" +
            "   </distributed-cache>\n" +
            "   <distributed-cache name=\"duplicatename\">\n" +
            "   </distributed-cache>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      TestCacheManagerFactory.fromStream(is);
   }

   public void testNoSchemaWithStuff() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "        <locking concurrency-level=\"10000\" isolation=\"REPEATABLE_READ\" />\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assertEquals(10000, cfg.locking().concurrencyLevel());
            assertEquals(IsolationLevel.REPEATABLE_READ, cfg.locking().isolationLevel());
         }

      });

   }

   public void testCompatibility() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assertFalse(cfg.compatibility().enabled());
            assertNull(cfg.compatibility().marshaller());
         }
      });

      config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <compatibility/>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );

      is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assertTrue(cfg.compatibility().enabled());
            assertNull(cfg.compatibility().marshaller());
         }
      });

      config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <compatibility marshaller=\"org.infinispan.commons.marshall.jboss.GenericJBossMarshaller\"/>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );

      is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assertTrue(cfg.compatibility().enabled());
            assertTrue(cfg.compatibility().marshaller() instanceof GenericJBossMarshaller);
         }
      });
   }

   public void testOffHeap() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <memory>\n" +
            "        <off-heap strategy=\"MANUAL\"/>\n" +
            "      </memory>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assertTrue(cfg.dataContainer().<byte[]>keyEquivalence() instanceof AnyEquivalence);
            assertTrue(cfg.dataContainer().valueEquivalence() instanceof AnyEquivalence);
            assertEquals(StorageType.OFF_HEAP, cfg.memory().storageType());
            assertEquals(EvictionStrategy.MANUAL, cfg.memory().evictionStrategy());
         }
      });

      config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <memory/>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );
      is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assertTrue(cfg.dataContainer().<byte[]>keyEquivalence() instanceof AnyEquivalence);
            assertTrue(cfg.dataContainer().<byte[]>valueEquivalence() instanceof AnyEquivalence);
            assertEquals(StorageType.OBJECT, cfg.memory().storageType());
         }
      });

      config = TestingUtil.wrapXMLWithoutSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <memory>\n" +
            "         <binary/>\n" +
            "      </memory>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );
      is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assertTrue(cfg.dataContainer().<byte[]>keyEquivalence() instanceof AnyEquivalence);
            assertTrue(cfg.dataContainer().<byte[]>valueEquivalence() instanceof AnyEquivalence);
            assertEquals(StorageType.BINARY, cfg.memory().storageType());
         }
      });
   }

   public void testDummyInMemoryStore() throws IOException {
      String config = TestingUtil.wrapXMLWithoutSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "<persistence >\n" +
               "<store class=\"org.infinispan.persistence.dummy.DummyInMemoryStore\" >\n" +
                  "<property name=\"storeName\">myStore</property>" +
               "</store >\n" +
            "</persistence >\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            PersistenceConfiguration cfg = cm.getDefaultCacheConfiguration().persistence();
            StoreConfiguration config = cfg.stores().get(0);
            assertTrue(config instanceof DummyInMemoryStoreConfiguration);
            DummyInMemoryStoreConfiguration dummyInMemoryStoreConfiguration = (DummyInMemoryStoreConfiguration)config;
            assertEquals("myStore", dummyInMemoryStoreConfiguration.storeName());
         }
      });
   }

   public static class GenericLoader implements CacheLoader {

      @Override
      public void init(InitializationContext ctx) { }

      @Override
      public MarshalledEntry load(Object key) { return null; }

      @Override
      public boolean contains(Object key) { return false; }

      @Override
      public void start() { }

      @Override
      public void stop() { }
   }

   public void testStoreWithNoConfigureBy() throws IOException {
      String config = TestingUtil.wrapXMLWithoutSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "<persistence >\n" +
               "<store class=\"org.infinispan.configuration.XmlFileParsingTest$GenericLoader\" preload=\"true\" >\n" +
                     "<property name=\"fetchPersistentState\">true</property>" +
               "</store >\n" +
            "</persistence >\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            PersistenceConfiguration cfg = cm.getDefaultCacheConfiguration().persistence();
            StoreConfiguration config = cfg.stores().get(0);
            assertTrue(config instanceof AbstractStoreConfiguration);
            AbstractStoreConfiguration abstractStoreConfiguration = (AbstractStoreConfiguration)config;
            assertTrue(abstractStoreConfiguration.fetchPersistentState());
            assertTrue(abstractStoreConfiguration.preload());
         }
      });
   }

   public void testCustomTransport() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema(
            "<jgroups transport=\"org.infinispan.configuration.XmlFileParsingTest$CustomTransport\"/>\n" +
            "<cache-container default-cache=\"default\">\n" +
            "  <transport cluster=\"ispn-perf-test\"/>\n" +
            "  <distributed-cache name=\"default\"/>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Transport transport = cm.getTransport();
            assertNotNull(transport);
            assertTrue(transport instanceof CustomTransport);
         }
      });
   }

   public void testNoDefaultCache() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
            "   <transport cluster=\"demoCluster\"/>\n" +
            "   <replicated-cache name=\"default\">\n" +
            "   </replicated-cache>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            GlobalConfiguration globalCfg = cm.getCacheManagerConfiguration();
            assertFalse(globalCfg.defaultCacheName().isPresent());
            assertNull(cm.getDefaultCacheConfiguration());
            assertEquals(CacheMode.REPL_SYNC, cm.getCacheConfiguration("default").clustering().cacheMode());
         }

      });
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000432:.*")
   public void testNoDefaultCacheDeclaration() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"non-existent\">" +
            "   <transport cluster=\"demoCluster\"/>\n" +
            "   <replicated-cache name=\"default\">\n" +
            "   </replicated-cache>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            // Do nothing
         }

      });
   }


   public void testWildcards() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
            "   <local-cache-configuration name=\"wildcache*\">\n" +
            "      <expiration interval=\"10500\" lifespan=\"11\" max-idle=\"11\"/>\n" +
            "   </local-cache-configuration>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            Configuration wildcache1 = cm.getCacheConfiguration("wildcache1");
            assertNotNull(wildcache1);
            assertEquals(10500, wildcache1.expiration().wakeUpInterval());
            assertEquals(11, wildcache1.expiration().lifespan());
            assertEquals(11, wildcache1.expiration().maxIdle());
         }

      });
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000485:.*")
   public void testAmbiguousWildcards() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
            "   <local-cache-configuration name=\"wildcache*\">\n" +
            "      <expiration interval=\"10500\" lifespan=\"11\" max-idle=\"11\"/>\n" +
            "   </local-cache-configuration>\n" +
            "   <local-cache-configuration name=\"wild*\">\n" +
            "   </local-cache-configuration>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            cm.getCacheConfiguration("wildcache1");
            fail("Ambiguous name should have thrown exception");
         }

      });
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000484:.*")
   public void testNoWildcardsInCacheName() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
            "   <transport cluster=\"demoCluster\"/>\n" +
            "   <replicated-cache name=\"wildcard*\">\n" +
            "   </replicated-cache>\n" +
            "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            fail("Should have failed earlier");
         }

      });
   }


   private void assertNamedCacheFile(ConfigurationBuilderHolder holder, boolean deprecated) {
      GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();

      BlockingThreadPoolExecutorFactory listenerThreadPool =
         gc.listenerThreadPool().threadPoolFactory();
      assertEquals(5, listenerThreadPool.maxThreads());
      assertEquals(10000, listenerThreadPool.queueLength());
      DefaultThreadFactory listenerThreadFactory =
         gc.listenerThreadPool().threadFactory();
      assertEquals("AsyncListenerThread", listenerThreadFactory.threadNamePattern());

      BlockingThreadPoolExecutorFactory persistenceThreadPool =
         gc.persistenceThreadPool().threadPoolFactory();
      assertEquals(6, persistenceThreadPool.maxThreads());
      assertEquals(10001, persistenceThreadPool.queueLength());
      DefaultThreadFactory persistenceThreadFactory =
         gc.persistenceThreadPool().threadFactory();
      assertEquals("PersistenceThread", persistenceThreadFactory.threadNamePattern());

      BlockingThreadPoolExecutorFactory asyncThreadPool =
         gc.asyncThreadPool().threadPoolFactory();
      assertEquals(5, asyncThreadPool.coreThreads());
      assertEquals(5, asyncThreadPool.maxThreads());
      assertEquals(10000, asyncThreadPool.queueLength());
      assertEquals(0, asyncThreadPool.keepAlive());
      DefaultThreadFactory asyncThreadFactory = gc.asyncThreadPool().threadFactory();
      assertEquals("AsyncOperationsThread", asyncThreadFactory.threadNamePattern());

      BlockingThreadPoolExecutorFactory transportThreadPool =
         gc.transport().transportThreadPool().threadPoolFactory();
      assertEquals(25, transportThreadPool.maxThreads());
      assertEquals(10000, transportThreadPool.queueLength());
      assertEquals(0, transportThreadPool.keepAlive());

      BlockingThreadPoolExecutorFactory remoteCommandThreadPool =
         gc.transport().remoteCommandThreadPool().threadPoolFactory();
      assertEquals(30, remoteCommandThreadPool.maxThreads());
      assertEquals(2, remoteCommandThreadPool.coreThreads());
      assertEquals(10000, remoteCommandThreadPool.queueLength());
      assertEquals(10000, remoteCommandThreadPool.keepAlive());

      BlockingThreadPoolExecutorFactory stateTransferThreadPool =
         gc.stateTransferThreadPool().threadPoolFactory();
      assertEquals(20, stateTransferThreadPool.maxThreads());
      assertEquals(2, remoteCommandThreadPool.coreThreads());
      assertEquals(5, stateTransferThreadPool.queueLength());
      assertEquals(60000, stateTransferThreadPool.keepAlive());

      DefaultThreadFactory evictionThreadFactory =
         gc.expirationThreadPool().threadFactory();
      assertEquals("ExpirationThread", evictionThreadFactory.threadNamePattern());

      assertTrue(gc.transport().transport() instanceof JGroupsTransport);
      assertEquals("infinispan-cluster", gc.transport().clusterName());
      assertEquals("Jalapeno", gc.transport().nodeName());
      assertEquals(50000, gc.transport().distributedSyncTimeout());

      assertEquals(ShutdownHookBehavior.REGISTER, gc.shutdown().hookBehavior());

      assertTrue(gc.serialization().marshaller() instanceof TestObjectStreamMarshaller);
      assertEquals(Version.getVersionShort("1.0"), gc.serialization().version());
      final Map<Integer, AdvancedExternalizer<?>> externalizers = gc.serialization().advancedExternalizers();
      assertEquals(3, externalizers.size());
      assertTrue(externalizers.get(1234) instanceof AdvancedExternalizerTest.IdViaConfigObj.Externalizer);
      assertTrue(externalizers.get(5678) instanceof AdvancedExternalizerTest.IdViaAnnotationObj.Externalizer);
      assertTrue(externalizers.get(3456) instanceof AdvancedExternalizerTest.IdViaBothObj.Externalizer);

      Configuration defaultCfg = holder.getDefaultConfigurationBuilder().build();

      assertEquals(1000, defaultCfg.locking().lockAcquisitionTimeout());
      assertEquals(100, defaultCfg.locking().concurrencyLevel());
      assertEquals(IsolationLevel.REPEATABLE_READ, defaultCfg.locking().isolationLevel());
      if (!deprecated) {
         assertReaperAndTimeoutInfo(defaultCfg);
      }


      Configuration c = holder.getNamedConfigurationBuilders().get("transactional").build();
      assertTrue(!c.clustering().cacheMode().isClustered());
      assertTrue(c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup);
      if (!deprecated) {
         assertReaperAndTimeoutInfo(defaultCfg);
      }

      c = holder.getNamedConfigurationBuilders().get("transactional2").build();
      assertTrue(c.transaction().transactionManagerLookup() instanceof TestLookup);
      assertEquals(10000, c.transaction().cacheStopTimeout());
      assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());
      assertTrue(!c.transaction().autoCommit());

      c = holder.getNamedConfigurationBuilders().get("transactional3").build();

      if (!deprecated) {
         assertEquals(TransactionProtocol.TOTAL_ORDER, c.transaction().transactionProtocol());
      }

      c = holder.getNamedConfigurationBuilders().get("syncInval").build();

      assertEquals(CacheMode.INVALIDATION_SYNC, c.clustering().cacheMode());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().remoteTimeout());

      c = holder.getNamedConfigurationBuilders().get("asyncInval").build();

      assertEquals(CacheMode.INVALIDATION_ASYNC, c.clustering().cacheMode());
      assertEquals(15000, c.clustering().remoteTimeout());

      c = holder.getNamedConfigurationBuilders().get("syncRepl").build();

      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertTrue(!c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().remoteTimeout());

      c = holder.getNamedConfigurationBuilders().get("asyncRepl").build();

      assertEquals(CacheMode.REPL_ASYNC, c.clustering().cacheMode());
      assertTrue(!c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());

      c = holder.getNamedConfigurationBuilders().get("txSyncRepl").build();

      assertTrue(c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup);
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertTrue(!c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().remoteTimeout());

      c = holder.getNamedConfigurationBuilders().get("overriding").build();

      assertEquals(CacheMode.LOCAL, c.clustering().cacheMode());
      assertEquals(20000, c.locking().lockAcquisitionTimeout());
      assertEquals(1000, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.REPEATABLE_READ, c.locking().isolationLevel());
      assertEquals(StorageType.OBJECT, c.memory().storageType());

      c = holder.getNamedConfigurationBuilders().get("storeAsBinary").build();
      assertEquals(StorageType.BINARY, c.memory().storageType());

      c = holder.getNamedConfigurationBuilders().get("withFileStore").build();
      assertTrue(c.persistence().preload());
      assertTrue(!c.persistence().passivation());
      assertEquals(1, c.persistence().stores().size());

      SingleFileStoreConfiguration loaderCfg = (SingleFileStoreConfiguration) c.persistence().stores().get(0);

      assertTrue(loaderCfg.fetchPersistentState());
      assertTrue(loaderCfg.ignoreModifications());
      assertTrue(loaderCfg.purgeOnStartup());
      assertEquals("/tmp/FileCacheStore-Location", loaderCfg.location());
      assertEquals(5, loaderCfg.async().threadPoolSize());
      assertTrue(loaderCfg.async().enabled());
      assertEquals(700, loaderCfg.async().modificationQueueSize());

      c = holder.getNamedConfigurationBuilders().get("withClusterLoader").build();
      assertEquals(1, c.persistence().stores().size());
      ClusterLoaderConfiguration clusterLoaderCfg = (ClusterLoaderConfiguration) c.persistence().stores().get(0);
      assertEquals(15000, clusterLoaderCfg.remoteCallTimeout());

      c = holder.getNamedConfigurationBuilders().get("withLoaderDefaults").build();
      loaderCfg = (SingleFileStoreConfiguration) c.persistence().stores().get(0);
      assertEquals("/tmp/Another-FileCacheStore-Location", loaderCfg.location());

      c = holder.getNamedConfigurationBuilders().get("withouthJmxEnabled").build();
      assertTrue(!c.jmxStatistics().enabled());
      assertTrue(gc.globalJmxStatistics().enabled());
      assertTrue(gc.globalJmxStatistics().allowDuplicateDomains());
      assertEquals("funky_domain", gc.globalJmxStatistics().domain());
      assertTrue(gc.globalJmxStatistics().mbeanServerLookup() instanceof PerThreadMBeanServerLookup);

      c = holder.getNamedConfigurationBuilders().get("dist").build();
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertEquals(600000, c.clustering().l1().lifespan());
      if (deprecated) assertEquals(120000, c.clustering().hash().rehashRpcTimeout());
      assertEquals(120000, c.clustering().stateTransfer().timeout());
      assertEquals(1200, c.clustering().l1().cleanupTaskFrequency());
      assertEquals(null, c.clustering().hash().consistentHash()); // this is just an override.
      assertEquals(3, c.clustering().hash().numOwners());
      assertTrue(c.clustering().l1().enabled());

      c = holder.getNamedConfigurationBuilders().get("dist_with_capacity_factors").build();
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertEquals(600000, c.clustering().l1().lifespan());
      if (deprecated) assertEquals(120000, c.clustering().hash().rehashRpcTimeout());
      assertEquals(120000, c.clustering().stateTransfer().timeout());
      assertEquals(null, c.clustering().hash().consistentHash()); // this is just an override.
      assertEquals(3, c.clustering().hash().numOwners());
      assertTrue(c.clustering().l1().enabled());
      assertEquals(0.0f, c.clustering().hash().capacityFactor());
      if (!deprecated) assertEquals(1000, c.clustering().hash().numSegments());

      c = holder.getNamedConfigurationBuilders().get("groups").build();
      assertTrue(c.clustering().hash().groups().enabled());
      assertEquals(1, c.clustering().hash().groups().groupers().size());
      assertEquals(String.class, c.clustering().hash().groups().groupers().get(0).getKeyType());

      c = holder.getNamedConfigurationBuilders().get("chunkSize").build();
      assertTrue(c.clustering().stateTransfer().fetchInMemoryState());
      assertEquals(120000, c.clustering().stateTransfer().timeout());
      assertEquals(1000, c.clustering().stateTransfer().chunkSize());

      c = holder.getNamedConfigurationBuilders().get("cacheWithCustomInterceptors").build();
      assertTrue(!c.customInterceptors().interceptors().isEmpty());
      assertEquals(6, c.customInterceptors().interceptors().size());
      for(InterceptorConfiguration i : c.customInterceptors().interceptors()) {
         if (i.asyncInterceptor() instanceof FooInterceptor) {
            assertEquals(i.properties().getProperty("foo"), "bar");
         }
      }

      c = holder.getNamedConfigurationBuilders().get("evictionCache").build();
      assertEquals(5000, c.memory().size());
      assertEquals(EvictionStrategy.REMOVE, c.memory().evictionStrategy());
      assertEquals(EvictionType.COUNT, c.memory().evictionType());
      assertEquals(StorageType.OBJECT, c.memory().storageType());
      assertEquals(60000, c.expiration().lifespan());
      assertEquals(1000, c.expiration().maxIdle());
      assertEquals(500, c.expiration().wakeUpInterval());

      c = holder.getNamedConfigurationBuilders().get("evictionMemoryExceptionCache").build();
      assertEquals(5000, c.memory().size());
      assertEquals(EvictionStrategy.EXCEPTION, c.memory().evictionStrategy());
      assertEquals(EvictionType.MEMORY, c.memory().evictionType());
      assertEquals(StorageType.BINARY, c.memory().storageType());

      c = holder.getNamedConfigurationBuilders().get("withDeadlockDetection").build();
      assertFalse(c.deadlockDetection().enabled());
      assertEquals(-1, c.deadlockDetection().spinDuration());
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());

      c = holder.getNamedConfigurationBuilders().get("storeKeyValueBinary").build();
      assertEquals(StorageType.BINARY, c.memory().storageType());
   }

   private void assertReaperAndTimeoutInfo(Configuration defaultCfg) {
      assertEquals(123, defaultCfg.transaction().reaperWakeUpInterval());
      assertEquals(3123, defaultCfg.transaction().completedTxTimeout());
   }

   public static class CustomTransport extends JGroupsTransport {

   }
}
