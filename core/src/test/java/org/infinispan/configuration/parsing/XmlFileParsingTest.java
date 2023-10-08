package org.infinispan.configuration.parsing;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.commons.test.Exceptions.expectException;
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
import java.util.Collection;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.EncodingConfiguration;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.threads.AbstractThreadPoolExecutorFactory;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.factories.threads.EnhancedQueueExecutorFactory;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.tx.TestLookup;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   @Test(expectedExceptions = FileNotFoundException.class)
   public void testFailOnMissingConfigurationFile() throws IOException {
      new DefaultCacheManager("does-not-exist.xml");
   }

   public void testNamedCacheFile() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/named-cache-test.xml");
      assertNamedCacheFile(holder, false);
   }

   public void testNoNamedCaches() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
                  "   <transport cluster=\"demoCluster\"/>\n" +
                  "   <replicated-cache name=\"default\">\n" +
                  "   </replicated-cache>\n" +
                  "</cache-container>"
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      GlobalConfiguration globalCfg = holder.getGlobalConfigurationBuilder().build();

      assertTrue(globalCfg.transport().transport() instanceof JGroupsTransport);
      assertEquals("demoCluster", globalCfg.transport().clusterName());

      Configuration cfg = holder.getDefaultConfigurationBuilder().build();
      assertEquals(CacheMode.REPL_SYNC, cfg.clustering().cacheMode());
   }

   private static ConfigurationBuilderHolder parseStringConfiguration(String config) {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      return parserRegistry.parse(is, ConfigurationResourceResolvers.DEFAULT, MediaType.APPLICATION_XML);
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testDuplicateCacheNames() {
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

   public void testNoSchemaWithStuff() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
                  "   <local-cache name=\"default\">\n" +
                  "        <locking concurrency-level=\"10000\" isolation=\"REPEATABLE_READ\" />\n" +
                  "   </local-cache>\n" +
                  "</cache-container>"
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      Configuration cfg = holder.getDefaultConfigurationBuilder().build();
      assertEquals(10000, cfg.locking().concurrencyLevel());
      assertEquals(IsolationLevel.REPEATABLE_READ, cfg.locking().lockIsolationLevel());

   }

   public void testOffHeap() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
                  "   <local-cache name=\"default\">\n" +
                  "      <memory storage=\"OFF_HEAP\" when-full=\"MANUAL\" />\n" +
                  "   </local-cache>\n" +
                  "</cache-container>"
      );
      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      Configuration cfg = holder.getDefaultConfigurationBuilder().build();
      assertEquals(StorageType.OFF_HEAP, cfg.memory().storageType());
      assertEquals(EvictionStrategy.MANUAL, cfg.memory().evictionStrategy());

      config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
                  "   <local-cache name=\"default\">\n" +
                  "      <memory/>\n" +
                  "   </local-cache>\n" +
                  "</cache-container>"
      );
      holder = parseStringConfiguration(config);
      cfg = holder.getDefaultConfigurationBuilder().build();
      assertEquals(StorageType.HEAP, cfg.memory().storageType());

      config = TestingUtil.wrapXMLWithoutSchema(
            "<cache-container default-cache=\"default\">" +
                  "   <local-cache name=\"default\">\n" +
                  "      <memory storage=\"BINARY\"/>\n" +
                  "   </local-cache>\n" +
                  "</cache-container>"
      );
      holder = parseStringConfiguration(config);
      cfg = holder.getDefaultConfigurationBuilder().build();
      assertEquals(StorageType.BINARY, cfg.memory().storageType());
   }

   public void testDummyInMemoryStore() {
      String config = TestingUtil.wrapXMLWithoutSchema(
            "<cache-container default-cache=\"default\">" +
                  "  <local-cache name=\"default\">\n" +
                  "    <persistence >\n" +
                  "      <dummy-store xmlns=\"urn:infinispan:config:store:dummy:" + Version.getSchemaVersion() + "\" store-name=\"myStore\" />\n" +
                  "    </persistence >\n" +
                  "  </local-cache>\n" +
                  "</cache-container>"
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      PersistenceConfiguration cfg = holder.getDefaultConfigurationBuilder().build().persistence();
      StoreConfiguration storeConfiguration = cfg.stores().get(0);
      assertTrue(storeConfiguration instanceof DummyInMemoryStoreConfiguration);
      DummyInMemoryStoreConfiguration dummyInMemoryStoreConfiguration = (DummyInMemoryStoreConfiguration) storeConfiguration;
      assertEquals("myStore", dummyInMemoryStoreConfiguration.storeName());
   }

   /**
    * Used by testStoreWithNoConfigureBy, although the cache is not really created.
    */
   @SuppressWarnings("unused")
   public static class GenericLoader implements CacheLoader {

      @Override
      public void init(InitializationContext ctx) {
      }

      @Override
      public MarshallableEntry loadEntry(Object key) {
         return null;
      }

      @Override
      public boolean contains(Object key) {
         return false;
      }

      @Override
      public void start() {
      }

      @Override
      public void stop() {
      }
   }

   public void testStoreWithNoConfigureBy() {
      String config = TestingUtil.wrapXMLWithoutSchema(
            "<cache-container default-cache=\"default\">" +
                  "   <local-cache name=\"default\">\n" +
                  "      <persistence >\n" +
                  "         <store class=\"" + GenericLoader.class.getName() + "\" preload=\"true\" fetch-state=\"true\" />\n" +
                  "      </persistence >\n" +
                  "   </local-cache>\n" +
                  "</cache-container>"
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      PersistenceConfiguration cfg = holder.getDefaultConfigurationBuilder().build().persistence();
      StoreConfiguration storeConfiguration = cfg.stores().get(0);
      assertTrue(storeConfiguration instanceof AbstractStoreConfiguration);
      AbstractStoreConfiguration abstractStoreConfiguration = (AbstractStoreConfiguration) storeConfiguration;
      assertTrue(abstractStoreConfiguration.preload());
   }

   public void testCustomTransport() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<jgroups transport=\"" + CustomTransport.class.getName() + "\"/>\n" +
                  "<cache-container default-cache=\"default\">\n" +
                  "  <transport cluster=\"ispn-perf-test\"/>\n" +
                  "  <distributed-cache name=\"default\"/>\n" +
                  "</cache-container>"
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      Transport transport = holder.getGlobalConfigurationBuilder().build().transport().transport();
      assertNotNull(transport);
      assertTrue(transport instanceof CustomTransport);
   }

   public void testNoDefaultCache() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
                  "   <transport cluster=\"demoCluster\"/>\n" +
                  "   <replicated-cache name=\"default\">\n" +
                  "   </replicated-cache>\n" +
                  "</cache-container>"
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      GlobalConfiguration globalCfg = holder.getGlobalConfigurationBuilder().build();
      assertFalse(globalCfg.defaultCacheName().isPresent());
      assertNull(holder.getDefaultConfigurationBuilder());
      assertEquals(CacheMode.REPL_SYNC, getCacheConfiguration(holder, "default").clustering().cacheMode());
   }

   private Configuration getCacheConfiguration(ConfigurationBuilderHolder holder, String cacheName) {
      return holder.getNamedConfigurationBuilders().get(cacheName).build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000432:.*")
   public void testNoDefaultCacheDeclaration() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"non-existent\">" +
                  "   <transport cluster=\"demoCluster\"/>\n" +
                  "   <replicated-cache name=\"default\">\n" +
                  "   </replicated-cache>\n" +
                  "</cache-container>"
      );

      parseStringConfiguration(config);
   }

   public void testNoCacheName() {
      String config =
            "<local-cache>\n" +
                  "   <expiration interval=\"10500\" lifespan=\"11\" max-idle=\"11\"/>\n" +
                  "</local-cache>";
      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      Configuration configuration = holder.getCurrentConfigurationBuilder().build();
      assertEquals(CacheMode.LOCAL, configuration.clustering().cacheMode());
      assertEquals(10500, configuration.expiration().wakeUpInterval());
      assertEquals(11, configuration.expiration().lifespan());
      assertEquals(11, configuration.expiration().maxIdle());
   }

   public void testWildcards() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
                  "   <local-cache-configuration name=\"wildcache*\">\n" +
                  "      <expiration interval=\"10500\" lifespan=\"11\" max-idle=\"11\"/>\n" +
                  "   </local-cache-configuration>\n" +
                  "</cache-container>"
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      try (DefaultCacheManager cm = new DefaultCacheManager(holder, false)) {
         Configuration wildcache1 = cm.getCacheConfiguration("wildcache1");
         assertEquals(10500, wildcache1.expiration().wakeUpInterval());
         assertEquals(11, wildcache1.expiration().lifespan());
         assertEquals(11, wildcache1.expiration().maxIdle());
      }
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000485:.*")
   public void testAmbiguousWildcards() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
                  "   <local-cache-configuration name=\"wildcache*\">\n" +
                  "   </local-cache-configuration>\n" +
                  "   <local-cache-configuration name=\"wild*\">\n" +
                  "   </local-cache-configuration>\n" +
                  "</cache-container>"
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      try (DefaultCacheManager cm = new DefaultCacheManager(holder, false)) {
         cm.getCacheConfiguration("wildcache1");
      }
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN000484:.*")
   public void testNoWildcardsInCacheName() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
                  "   <transport cluster=\"demoCluster\"/>\n" +
                  "   <replicated-cache name=\"wildcard*\">\n" +
                  "   </replicated-cache>\n" +
                  "</cache-container>"
      );

      parseStringConfiguration(config);
      fail("Should have failed earlier");
   }

   public void testAsyncInheritance() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
                  "   <transport cluster=\"demoCluster\"/>\n" +
                  "   <replicated-cache-configuration mode=\"ASYNC\" name=\"repl-1\">\n" +
                  "   </replicated-cache-configuration>\n" +
                  "   <replicated-cache-configuration name=\"repl-2\" configuration=\"repl-1\">\n" +
                  "   </replicated-cache-configuration>\n" +
                  "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      ConfigurationBuilderHolder holder = TestCacheManagerFactory.parseStream(is, false);
      Configuration repl1 = getCacheConfiguration(holder, "repl-1");
      Configuration repl2 = getCacheConfiguration(holder, "repl-2");
      assertTrue(repl1.isTemplate());
      assertTrue(repl2.isTemplate());
      assertEquals(CacheMode.REPL_ASYNC, repl1.clustering().cacheMode());
      assertEquals(CacheMode.REPL_ASYNC, repl2.clustering().cacheMode());
   }

   public void testInlineJGroupsStack() throws IOException {
      try (DefaultCacheManager cm = new DefaultCacheManager("configs/config-with-jgroups-stack.xml")) {
         assertTrue(cm.isCoordinator());
      }
   }

   public void testRaftMembersParsing() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
                  "   <transport cluster=\"node-name-missing\" raft-members=\"a b c\" node-name=\"a\"/>\n" +
                  "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      ConfigurationBuilderHolder holder = TestCacheManagerFactory.parseStream(is, false);
      GlobalConfiguration configuration = holder.getGlobalConfigurationBuilder().build();
      Collection<String> raftMembers = configuration.transport().raftMembers();

      assertEquals(3, raftMembers.size());
      assertTrue(raftMembers.contains("a"));
      assertTrue(raftMembers.contains("b"));
      assertTrue(raftMembers.contains("c"));

      assertEquals("a", configuration.transport().nodeName());
   }

   public void testNodeNameMissingWithRaft() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>\n" +
                  "   <transport cluster=\"node-name-missing\" raft-members=\"a b c\"/>\n" +
                  "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      expectException(CacheConfigurationException.class, "ISPN000667:.*", () -> TestCacheManagerFactory.parseStream(is, false));

   }

   public void testInvalidTracingCollector() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container name=\"default\">\n" +
                  "      <tracing collector-endpoint=\"sdjsd92k2..21232\" />\n" +
                  "   </cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      expectException(CacheConfigurationException.class, "ISPN000699:.*", () -> TestCacheManagerFactory.parseStream(is, false));
   }

   public void testNodeNameNotInRaftMembers() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
                  "   <transport cluster=\"node-name-missing\" raft-members=\"a b c\" node-name=\"d\"/>\n" +
                  "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      expectException(CacheConfigurationException.class, "ISPN000668:.*", () -> TestCacheManagerFactory.parseStream(is, false));

   }

   private void assertNamedCacheFile(ConfigurationBuilderHolder holder, boolean deprecated) {
      GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();

      EnhancedQueueExecutorFactory listenerThreadPool =
            gc.listenerThreadPool().threadPoolFactory();
      assertEquals(5, listenerThreadPool.maxThreads());
      assertEquals(10000, listenerThreadPool.queueLength());
      DefaultThreadFactory listenerThreadFactory =
            gc.listenerThreadPool().threadFactory();
      assertEquals("AsyncListenerThread", listenerThreadFactory.threadNamePattern());

      AbstractThreadPoolExecutorFactory persistenceThreadPool =
            gc.persistenceThreadPool().threadPoolFactory();
      assertNull(persistenceThreadPool);

      AbstractThreadPoolExecutorFactory blockingThreadPool =
            gc.blockingThreadPool().threadPoolFactory();
      assertEquals(6, blockingThreadPool.maxThreads());
      assertEquals(10001, blockingThreadPool.queueLength());
      DefaultThreadFactory persistenceThreadFactory =
            gc.blockingThreadPool().threadFactory();
      assertEquals("BlockingThread", persistenceThreadFactory.threadNamePattern());

      AbstractThreadPoolExecutorFactory asyncThreadPool =
            gc.asyncThreadPool().threadPoolFactory();
      assertNull(asyncThreadPool);

      AbstractThreadPoolExecutorFactory nonBlockingThreadPool =
            gc.nonBlockingThreadPool().threadPoolFactory();
      assertEquals(5, nonBlockingThreadPool.coreThreads());
      assertEquals(5, nonBlockingThreadPool.maxThreads());
      assertEquals(10000, nonBlockingThreadPool.queueLength());
      assertEquals(0, nonBlockingThreadPool.keepAlive());
      DefaultThreadFactory asyncThreadFactory = gc.nonBlockingThreadPool().threadFactory();
      assertEquals("NonBlockingThread", asyncThreadFactory.threadNamePattern());

      AbstractThreadPoolExecutorFactory transportThreadPool =
            gc.transport().transportThreadPool().threadPoolFactory();
      assertNull(transportThreadPool);

      AbstractThreadPoolExecutorFactory remoteCommandThreadPool =
            gc.transport().remoteCommandThreadPool().threadPoolFactory();
      assertNull(remoteCommandThreadPool);

      DefaultThreadFactory evictionThreadFactory =
            gc.expirationThreadPool().threadFactory();
      assertEquals("ExpirationThread", evictionThreadFactory.threadNamePattern());

      assertTrue(gc.transport().transport() instanceof JGroupsTransport);
      assertEquals("infinispan-cluster", gc.transport().clusterName());
      assertEquals("Jalapeno", gc.transport().nodeName());
      assertEquals(50000, gc.transport().distributedSyncTimeout());

      assertEquals(ShutdownHookBehavior.REGISTER, gc.shutdown().hookBehavior());

      assertTrue(gc.serialization().marshaller() instanceof TestObjectStreamMarshaller);

      Configuration defaultCfg = holder.getDefaultConfigurationBuilder().build();
      assertEquals(1000, defaultCfg.locking().lockAcquisitionTimeout());
      assertEquals(100, defaultCfg.locking().concurrencyLevel());
      assertEquals(IsolationLevel.REPEATABLE_READ, defaultCfg.locking().lockIsolationLevel());
      if (!deprecated) {
         assertReaperAndTimeoutInfo(defaultCfg);
      }


      Configuration c = getCacheConfiguration(holder, "transactional");
      assertFalse(c.clustering().cacheMode().isClustered());
      assertTrue(c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup);
      if (!deprecated) {
         assertReaperAndTimeoutInfo(defaultCfg);
      }

      c = getCacheConfiguration(holder, "transactional2");
      assertTrue(c.transaction().transactionManagerLookup() instanceof TestLookup);
      assertEquals(10000, c.transaction().cacheStopTimeout());
      assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());
      assertFalse(c.transaction().autoCommit());

      c = getCacheConfiguration(holder, "syncInval");

      assertEquals(CacheMode.INVALIDATION_SYNC, c.clustering().cacheMode());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().remoteTimeout());

      c = getCacheConfiguration(holder, "asyncInval");

      assertEquals(CacheMode.INVALIDATION_ASYNC, c.clustering().cacheMode());
      assertEquals(15000, c.clustering().remoteTimeout());

      c = getCacheConfiguration(holder, "syncRepl");

      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertFalse(c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().remoteTimeout());

      c = getCacheConfiguration(holder, "asyncRepl");

      assertEquals(CacheMode.REPL_ASYNC, c.clustering().cacheMode());
      assertFalse(c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());

      c = getCacheConfiguration(holder, "txSyncRepl");

      assertTrue(c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup);
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertFalse(c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().remoteTimeout());

      c = getCacheConfiguration(holder, "overriding");

      assertEquals(CacheMode.LOCAL, c.clustering().cacheMode());
      assertEquals(20000, c.locking().lockAcquisitionTimeout());
      assertEquals(1000, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.REPEATABLE_READ, c.locking().lockIsolationLevel());
      assertEquals(StorageType.HEAP, c.memory().storageType());

      c = getCacheConfiguration(holder, "storeAsBinary");
      assertEquals(StorageType.BINARY, c.memory().storageType());

      c = getCacheConfiguration(holder, "withFileStore");
      assertTrue(c.persistence().preload());
      assertFalse(c.persistence().passivation());
      assertEquals(1, c.persistence().stores().size());

      SoftIndexFileStoreConfiguration loaderCfg = (SoftIndexFileStoreConfiguration) c.persistence().stores().get(0);

      assertFalse(loaderCfg.ignoreModifications());
      assertFalse(loaderCfg.purgeOnStartup());
      assertEquals("/tmp/FileCacheStore-Location", loaderCfg.dataLocation());
      assertTrue(loaderCfg.async().enabled());
      assertEquals(700, loaderCfg.async().modificationQueueSize());

      c = getCacheConfiguration(holder, "withClusterLoader");
      assertEquals(1, c.persistence().stores().size());
      ClusterLoaderConfiguration clusterLoaderCfg = (ClusterLoaderConfiguration) c.persistence().stores().get(0);
      assertEquals(15000, clusterLoaderCfg.remoteCallTimeout());

      c = getCacheConfiguration(holder, "withLoaderDefaults");
      loaderCfg = (SoftIndexFileStoreConfiguration) c.persistence().stores().get(0);
      assertEquals("/tmp/Another-FileCacheStore-Location", loaderCfg.dataLocation());

      c = getCacheConfiguration(holder, "withouthJmxEnabled");
      assertFalse(c.statistics().enabled());
      assertTrue(gc.statistics());
      assertTrue(gc.jmx().enabled());
      assertEquals("funky_domain", gc.jmx().domain());
      assertTrue(gc.jmx().mbeanServerLookup() instanceof TestMBeanServerLookup);

      c = getCacheConfiguration(holder, "dist");
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertEquals(600000, c.clustering().l1().lifespan());
      assertEquals(120000, c.clustering().stateTransfer().timeout());
      assertEquals(1200, c.clustering().l1().cleanupTaskFrequency());
      assertTrue(c.clustering().hash().consistentHashFactory() instanceof DefaultConsistentHashFactory);
      assertEquals(3, c.clustering().hash().numOwners());
      assertTrue(c.clustering().l1().enabled());

      c = getCacheConfiguration(holder, "dist_with_capacity_factors");
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertEquals(600000, c.clustering().l1().lifespan());
      assertEquals(120000, c.clustering().stateTransfer().timeout());
      assertNull(c.clustering().hash().consistentHashFactory());
      assertEquals(3, c.clustering().hash().numOwners());
      assertTrue(c.clustering().l1().enabled());
      assertEquals(0.0f, c.clustering().hash().capacityFactor());
      if (!deprecated) assertEquals(1000, c.clustering().hash().numSegments());

      c = getCacheConfiguration(holder, "groups");
      assertTrue(c.clustering().hash().groups().enabled());
      assertEquals(1, c.clustering().hash().groups().groupers().size());
      assertEquals(String.class, c.clustering().hash().groups().groupers().get(0).getKeyType());

      c = getCacheConfiguration(holder, "chunkSize");
      assertTrue(c.clustering().stateTransfer().fetchInMemoryState());
      assertEquals(120000, c.clustering().stateTransfer().timeout());
      assertEquals(1000, c.clustering().stateTransfer().chunkSize());

      c = getCacheConfiguration(holder, "evictionCache");
      assertEquals(5000, c.memory().size());
      assertEquals(EvictionStrategy.REMOVE, c.memory().evictionStrategy());
      assertEquals(EvictionType.COUNT, c.memory().evictionType());
      assertEquals(StorageType.OBJECT, c.memory().storageType());
      assertEquals(60000, c.expiration().lifespan());
      assertEquals(1000, c.expiration().maxIdle());
      assertEquals(500, c.expiration().wakeUpInterval());

      c = getCacheConfiguration(holder, "evictionMemoryExceptionCache");
      assertEquals(5000, c.memory().size());
      assertEquals(EvictionStrategy.EXCEPTION, c.memory().evictionStrategy());
      assertEquals(EvictionType.MEMORY, c.memory().evictionType());
      assertEquals(StorageType.BINARY, c.memory().storageType());

      c = getCacheConfiguration(holder, "storeKeyValueBinary");
      assertEquals(StorageType.BINARY, c.memory().storageType());
   }

   private void assertReaperAndTimeoutInfo(Configuration defaultCfg) {
      assertEquals(123, defaultCfg.transaction().reaperWakeUpInterval());
      assertEquals(3123, defaultCfg.transaction().completedTxTimeout());
   }

   public void testErrorReporting() {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      Exceptions.expectException("^ISPN000327:.*broken.xml\\[13,18\\].*", () -> parserRegistry.parseFile("configs/broken.xml"), CacheConfigurationException.class);
   }

   public void testEncodingMatching() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>\n" +
                  "<distributed-cache name=\"encoded-a\"><encoding media-type=\"application/x-protostream\"/></distributed-cache>\n" +
                  "<distributed-cache name=\"encoded-b\"><encoding><key media-type=\"application/x-protostream\"/><value media-type=\"application/x-protostream\"/></encoding></distributed-cache>\n" +
                  "</cache-container>"
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      EncodingConfiguration a = holder.getNamedConfigurationBuilders().get("encoded-a").build().encoding();
      EncodingConfiguration b = holder.getNamedConfigurationBuilders().get("encoded-b").build().encoding();
      assertTrue(a.matches(b));
   }

   public void testOrdering() {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container>" +
                  "   <transport cluster=\"demoCluster\"/>\n" +
                  "   <replicated-cache-configuration name=\"repl-2\" configuration=\"repl-1\">\n" +
                  "   </replicated-cache-configuration>\n" +
                  "   <replicated-cache-configuration mode=\"ASYNC\" name=\"repl-1\">\n" +
                  "   </replicated-cache-configuration>\n" +
                  "</cache-container>"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      ConfigurationBuilderHolder holder = TestCacheManagerFactory.parseStream(is, false);
      Configuration repl1 = getCacheConfiguration(holder, "repl-1");
      Configuration repl2 = getCacheConfiguration(holder, "repl-2");
      assertTrue(repl1.isTemplate());
      assertTrue(repl2.isTemplate());
      assertEquals(CacheMode.REPL_ASYNC, repl1.clustering().cacheMode());
      assertEquals(CacheMode.REPL_ASYNC, repl2.clustering().cacheMode());
   }

   public void testFragments() {
      testFragment0(
            """
                  <local-cache name="mycache"/>
                  """,
            CacheMode.LOCAL, false);
      testFragment0(
            """
                  <local-cache-configuration name="mycache"/>
                  """,
            CacheMode.LOCAL, true);
      testFragment0(
            """
                  <distributed-cache name="mycache"/>
                  """,
            CacheMode.DIST_SYNC, false);
      testFragment0(
            """
                  <distributed-cache-configuration name="mycache"/>
                  """,
            CacheMode.DIST_SYNC, true);
      testFragment0(
            """
                  <invalidation-cache name="mycache"/>
                  """,
            CacheMode.INVALIDATION_SYNC, false);
      testFragment0(
            """
                  <invalidation-cache-configuration name="mycache"/>
                  """,
            CacheMode.INVALIDATION_SYNC, true);
      testFragment0(
            """
                  <replicated-cache name="mycache"/>
                  """,
            CacheMode.REPL_SYNC, false);
      testFragment0(
            """
                  <replicated-cache-configuration name="mycache"/>
                  """,
            CacheMode.REPL_SYNC, true);
   }

   private void testFragment0(String config, CacheMode mode, boolean isTemplate) {
      ParserRegistry registry = new ParserRegistry();
      ConfigurationBuilderHolder holder = registry.parse(config);
      Configuration cfg = holder.getNamedConfigurationBuilders().get("mycache").build();
      assertEquals(mode, cfg.clustering().cacheMode());
      assertEquals(isTemplate, cfg.isTemplate());
   }

   public void testRemovedElement() {
      String config = TestingUtil.wrapXMLWithSchema(
            """
               <cache-container>
                  <scattered-cache/>
               </cache-container>
            """
      );
      assertThatThrownBy(() -> parseStringConfiguration(config)).hasMessage("ISPN000622: Element 'scattered-cache' at [4,25] has been removed with no replacement");
   }

   public void testRemovedAttribute() {
      String config = TestingUtil.wrapXMLWithSchema(
            """
               <cache-container>
                  <serialization version="1"/>
               </cache-container>
            """
      );
      assertThatThrownBy(() -> parseStringConfiguration(config)).hasMessage("ISPN000624: Attribute 'version' of element 'serialization' at '[4,35]' has been removed with no replacement");
   }

   public static class CustomTransport extends JGroupsTransport {

   }
}
