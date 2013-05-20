/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG_40;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG_NO_SCHEMA;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.infinispan.Version;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterCacheLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.FileCacheStoreConfiguration;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.interceptors.FooInterceptor;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.AdvancedExternalizerTest;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.tx.TestLookup;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.util.AnyEquivalence;
import org.infinispan.util.ByteArrayEquivalence;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   public void testOneLetterMode() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <clustering mode=\"r\">\n" +
            "      </clustering>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      assertCacheMode(config);
   }

   private void assertCacheMode(String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            Assert.assertEquals(cm.getDefaultCacheConfiguration().clustering().cacheMode(), CacheMode.REPL_SYNC);
         }
      });

   }

   public void testShortMode() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <clustering mode=\"repl\">\n" +
            "      </clustering>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      assertCacheMode(config);
   }

   @Test(expectedExceptions=FileNotFoundException.class)
   public void testFailOnUnexpectedConfigurationFile() throws IOException {
      TestCacheManagerFactory.fromXml("does-not-exist.xml");
   }

   public void testDeprecatedNonsenseMode() throws Exception {
      // TODO When we remove the nonsense mode, this test should be deleted
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <clustering mode=\"raphael\">\n" +
            "      </clustering>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      assertCacheMode(config);
   }

   public void testNamedCacheFile() throws IOException {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-test.xml", true)) {
         @Override
         public void call() {
            assertNamedCacheFile(cm, false);
         }
      });
   }

   public void testOldNamedCacheFile() throws IOException {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-test-51.xml", true)) {
         @Override
         public void call() {
            assertNamedCacheFile(cm, true);
         }
      });
   }

   public void testNoNamedCaches() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <global>\n" +
            "      <transport clusterName=\"demoCluster\"/>\n" +
            "   </global>\n" +
            "\n" +
            "   <default>\n" +
            "      <clustering mode=\"replication\">\n" +
            "      </clustering>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            GlobalConfiguration globalCfg = cm.getCacheManagerConfiguration();

            assert globalCfg.transport().transport() instanceof JGroupsTransport;
            assert globalCfg.transport().clusterName().equals("demoCluster");

            Configuration cfg = cm.getDefaultCacheConfiguration();
            assert cfg.clustering().cacheMode() == CacheMode.REPL_SYNC;
         }

      });

   }

   @Test(expectedExceptions = ConfigurationException.class)
   public void testBackwardCompatibleInputCacheConfiguration() throws Exception {
      // Read 4.0 configuration file against 4.1 schema
      String config = INFINISPAN_START_TAG_40 +
            "   <global>\n" +
            "      <transport clusterName=\"demoCluster\"/>\n" +
            "   </global>\n" +
            "\n" +
            "   <default>\n" +
            "      <clustering mode=\"replication\">\n" +
            "      </clustering>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)));
   }

   public void testNoSchemaWithStuff() throws IOException {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "    <default>\n" +
            "        <locking concurrencyLevel=\"10000\" isolationLevel=\"REPEATABLE_READ\" />\n" +
            "    </default>\n" +
            INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {

         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assert cfg.locking().concurrencyLevel() == 10000;
            assert cfg.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
         }

      });

   }

   public void testPassivationOnDefaultEvictionOnNamed() throws Exception {

      //should not throw a warning id 152 for each named caches, just for default
      //https://issues.jboss.org/browse/ISPN-1938
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
      "<default>\n" +
      "<transaction \n" +
      "transactionManagerLookupClass=\"org.infinispan.transaction.lookup.GenericTransactionManagerLookup\" \n" +
      "syncRollbackPhase=\"false\" syncCommitPhase=\"false\" useEagerLocking=\"false\" />\n" +
      "<loaders passivation=\"true\" shared=\"true\" preload=\"true\"> \n" +
      "<loader class=\"org.infinispan.loaders.file.FileCacheStore\" \n" +
      "fetchPersistentState=\"true\" purgerThreads=\"3\" purgeSynchronously=\"true\" \n" +
      "ignoreModifications=\"false\" purgeOnStartup=\"false\"> \n" +
      "</loader>\n" +
      "</loaders>\n" +
      "</default>\n" +
      "<namedCache name=\"Cache1\"> \n" +
      "<jmxStatistics enabled=\"true\" />\n" +
      "<eviction strategy=\"LIRS\" maxEntries=\"60000\" />\n" +
      "</namedCache> \n" +
      "<namedCache name=\"Cache2\"> \n" +
      "<jmxStatistics enabled=\"true\" />\n" +
      "<eviction strategy=\"LIRS\" maxEntries=\"60000\" />\n" +
      "</namedCache> \n" + INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)));
   }

   public void testVersioning() throws Exception {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<locking isolationLevel=\"REPEATABLE_READ\" lockAcquisitionTimeout=\"15000\" writeSkewCheck=\"true\"/>\n" +
            "<transaction transactionManagerLookupClass=\"org.infinispan.transaction.lookup.GenericTransactionManagerLookup\" transactionMode=\"TRANSACTIONAL\" lockingMode=\"OPTIMISTIC\"/>\n" +
            "<invocationBatching enabled=\"true\"/>\n" +
            "<versioning versioningScheme=\"SIMPLE\" enabled=\"true\"/>\n" +
            "<clustering mode=\"LOCAL\"/>\n" +
            "</default>\n" +
            INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            cm.getDefaultCacheConfiguration();
         }
      });
   }

   public void testDefensive() throws IOException {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<storeAsBinary enabled=\"true\" defensive=\"true\" />\n" +
            "</default>\n" +
            INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assert cfg.storeAsBinary().enabled();
            assert cfg.storeAsBinary().defensive();
         }
      });

      config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<storeAsBinary enabled=\"true\" defensive=\"false\" />\n" +
            "</default>\n" +
            INFINISPAN_END_TAG;
      is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assert cfg.storeAsBinary().enabled();
            assert !cfg.storeAsBinary().defensive();
         }
      });
   }

   public void testCompatibility() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <compatibility enabled=\"false\"/>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            Assert.assertFalse(cfg.compatibility().enabled());
            Assert.assertNull(cfg.compatibility().marshaller());
         }
      });

      config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <compatibility enabled=\"true\"/>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            Assert.assertTrue(cfg.compatibility().enabled());
            Assert.assertNull(cfg.compatibility().marshaller());
         }
      });

      config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <compatibility enabled=\"true\" marshallerClass=\"org.infinispan.marshall.jboss.GenericJBossMarshaller\"/>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            Assert.assertTrue(cfg.compatibility().enabled());
            Assert.assertTrue(cfg.compatibility().marshaller() instanceof GenericJBossMarshaller);
         }
      });

   }

   public void testKeyValueEquivalence() throws Exception {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<dataContainer keyEquivalence=\"org.infinispan.util.ByteArrayEquivalence\"/>\n" +
            "</default>\n" +
            INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assert cfg.dataContainer().keyEquivalence() instanceof ByteArrayEquivalence;
            assert cfg.dataContainer().valueEquivalence() instanceof AnyEquivalence;
         }
      });

      config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<dataContainer keyEquivalence=\"org.infinispan.util.ByteArrayEquivalence\" " +
                            "valueEquivalence=\"org.infinispan.util.ByteArrayEquivalence\" />\n" +
            "</default>\n" +
            INFINISPAN_END_TAG;
      is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Configuration cfg = cm.getDefaultCacheConfiguration();
            assert cfg.dataContainer().keyEquivalence() instanceof ByteArrayEquivalence;
            assert cfg.dataContainer().valueEquivalence() instanceof ByteArrayEquivalence;
         }
      });
   }

   private void assertNamedCacheFile(EmbeddedCacheManager cm, boolean deprecated) {
      final GlobalConfiguration gc = cm.getCacheManagerConfiguration();

      assert gc.asyncListenerExecutor().factory() instanceof DefaultExecutorFactory;
      assert gc.asyncListenerExecutor().properties().getProperty("maxThreads").equals("5");
      if (!deprecated) {
         assertEquals("10000", gc.asyncListenerExecutor().properties().getProperty("queueSize"));
      }
      assert gc.asyncListenerExecutor().properties().getProperty("threadNamePrefix").equals("AsyncListenerThread");

      assert gc.asyncTransportExecutor().factory() instanceof DefaultExecutorFactory;
      // Should be 25, but it's overriden by the test cache manager factory
      assertEquals(String.valueOf(TestCacheManagerFactory.MAX_ASYNC_EXEC_THREADS), gc.asyncTransportExecutor().properties().getProperty("maxThreads"));
      if (!deprecated) {
         assertEquals(String.valueOf(TestCacheManagerFactory.ASYNC_EXEC_QUEUE_SIZE), gc.asyncTransportExecutor().properties().getProperty("queueSize"));
      }
      assert gc.asyncTransportExecutor().properties().getProperty("threadNamePrefix").equals("AsyncSerializationThread");

      if (!deprecated) {
         assertTrue(gc.remoteCommandsExecutor().factory() instanceof DefaultExecutorFactory);
         assertEquals(String.valueOf(TestCacheManagerFactory.MAX_REQ_EXEC_THREADS),
                      gc.remoteCommandsExecutor().properties().getProperty("maxThreads"));
         assertEquals("RemoteCommandThread", gc.remoteCommandsExecutor().properties().getProperty("threadNamePrefix"));
         assertEquals("2", gc.remoteCommandsExecutor().properties().getProperty("coreThreads"));
         assertEquals(String.valueOf(TestCacheManagerFactory.KEEP_ALIVE), gc.remoteCommandsExecutor().properties().getProperty("keepAliveTime"));
      }

      if (!deprecated) {
         assertTrue(gc.totalOrderExecutor().factory() instanceof DefaultExecutorFactory);
         assertEquals("16", gc.totalOrderExecutor().properties().getProperty("maxThreads"));
         assertEquals("TotalOrderValidatorThread", gc.totalOrderExecutor().properties().getProperty("threadNamePrefix"));
         assertEquals("1", gc.totalOrderExecutor().properties().getProperty("coreThreads"));
         assertEquals("1000", gc.totalOrderExecutor().properties().getProperty("keepAliveTime"));
         assertEquals("0", gc.totalOrderExecutor().properties().getProperty("queueSize"));
      }

      assert gc.evictionScheduledExecutor().factory() instanceof DefaultScheduledExecutorFactory;
      assert gc.evictionScheduledExecutor().properties().getProperty("threadNamePrefix").equals("EvictionThread");

      assert gc.replicationQueueScheduledExecutor().factory() instanceof DefaultScheduledExecutorFactory;
      assert gc.replicationQueueScheduledExecutor().properties().getProperty("threadNamePrefix").equals("ReplicationQueueThread");

      assert gc.transport().transport() instanceof JGroupsTransport;
      assert gc.transport().clusterName().equals("infinispan-cluster");
      // Should be "Jalapeno" but it's overriden by the test cache manager factory
      assert gc.transport().nodeName().contains("Node");
      assert gc.transport().distributedSyncTimeout() == 50000;

      assert gc.shutdown().hookBehavior().equals(ShutdownHookBehavior.REGISTER);

      assert gc.serialization().marshaller() instanceof VersionAwareMarshaller;
      assert gc.serialization().version() == Version.getVersionShort("1.0");
      final Map<Integer, AdvancedExternalizer<?>> externalizers = gc.serialization().advancedExternalizers();
      assert externalizers.size() == 3;
      assert externalizers.get(1234) instanceof AdvancedExternalizerTest.IdViaConfigObj.Externalizer;
      assert externalizers.get(5678) instanceof AdvancedExternalizerTest.IdViaAnnotationObj.Externalizer;
      assert externalizers.get(3456) instanceof AdvancedExternalizerTest.IdViaBothObj.Externalizer;

      Configuration defaultCfg = cm.getDefaultCacheConfiguration();

      assert defaultCfg.locking().lockAcquisitionTimeout() == 1000;
      assert defaultCfg.locking().concurrencyLevel() == 100;
      assert defaultCfg.locking().isolationLevel() == IsolationLevel.READ_COMMITTED;
      if (!deprecated) {
         assertReaperAndTimeoutInfo(defaultCfg);
      }


      Configuration c = cm.getCacheConfiguration("transactional");
      assert !c.clustering().cacheMode().isClustered();
      assert c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup;
      assert c.transaction().useEagerLocking();
      assert c.transaction().eagerLockingSingleNode();
      assert !c.transaction().syncRollbackPhase();
      if (!deprecated) {
         assertReaperAndTimeoutInfo(defaultCfg);
      }

      c = cm.getCacheConfiguration("transactional2");
      assert c.transaction().transactionManagerLookup() instanceof TestLookup;
      assert c.transaction().cacheStopTimeout() == 10000;
      assert c.transaction().lockingMode().equals(LockingMode.PESSIMISTIC);
      assert !c.transaction().autoCommit();

      c = cm.getCacheConfiguration("transactional3");

      if (!deprecated) {
         Assert.assertEquals(c.transaction().transactionProtocol(), TransactionProtocol.TOTAL_ORDER);
      }

      c = cm.getCacheConfiguration("syncRepl");

      assert c.clustering().cacheMode() == CacheMode.REPL_SYNC;
      assert !c.clustering().stateTransfer().fetchInMemoryState();
      assert c.clustering().sync().replTimeout() == 15000;

      c = cm.getCacheConfiguration("asyncRepl");

      assert c.clustering().cacheMode() == CacheMode.REPL_ASYNC;
      assert !c.clustering().async().useReplQueue();
      assert !c.clustering().async().asyncMarshalling();
      assert !c.clustering().stateTransfer().fetchInMemoryState();

      c = cm.getCacheConfiguration("asyncReplQueue");

      assert c.clustering().cacheMode() == CacheMode.REPL_ASYNC;
      assert c.clustering().async().useReplQueue();
      assert !c.clustering().async().asyncMarshalling();
      assert !c.clustering().stateTransfer().fetchInMemoryState();

      c = cm.getCacheConfiguration("txSyncRepl");

      assert c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup;
      assert c.clustering().cacheMode() == CacheMode.REPL_SYNC;
      assert !c.clustering().stateTransfer().fetchInMemoryState();
      assert c.clustering().sync().replTimeout() == 15000;

      c = cm.getCacheConfiguration("overriding");

      assert c.clustering().cacheMode() == CacheMode.LOCAL;
      assert c.locking().lockAcquisitionTimeout() == 20000;
      assert c.locking().concurrencyLevel() == 1000;
      assert c.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      assert !c.storeAsBinary().enabled();

      c = cm.getCacheConfiguration("storeAsBinary");
      assert c.storeAsBinary().enabled();

      c = cm.getCacheConfiguration("withFileStore");
      assert c.loaders().preload();
      assert !c.loaders().passivation();
      assert !c.loaders().shared();
      assert c.loaders().cacheLoaders().size() == 1;

      FileCacheStoreConfiguration loaderCfg = (FileCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);

      assert loaderCfg.fetchPersistentState();
      assert loaderCfg.ignoreModifications();
      assert loaderCfg.purgeOnStartup();
      assertEquals("/tmp/FileCacheStore-Location", loaderCfg.location());
      assert loaderCfg.fsyncMode() == FileCacheStoreConfigurationBuilder.FsyncMode.PERIODIC;
      assert loaderCfg.fsyncInterval() == 2000;
      assert loaderCfg.singletonStore().pushStateTimeout() == 20000;
      assert loaderCfg.singletonStore().pushStateWhenCoordinator();
      assert loaderCfg.async().threadPoolSize() == 5;
      assert loaderCfg.async().flushLockTimeout() == 15000;
      assert loaderCfg.async().enabled();
      assert loaderCfg.async().modificationQueueSize() == 700;

      c = cm.getCacheConfiguration("withClusterLoader");
      assert c.loaders().cacheLoaders().size() == 1;
      ClusterCacheLoaderConfiguration clusterLoaderCfg = (ClusterCacheLoaderConfiguration) c.loaders().cacheLoaders().get(0);
      assert clusterLoaderCfg.remoteCallTimeout() == 15000;

      c = cm.getCacheConfiguration("withLoaderDefaults");
      loaderCfg = (FileCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);
      assert loaderCfg.location().equals("/tmp/Another-FileCacheStore-Location");
      assert loaderCfg.fsyncMode() == FileCacheStoreConfigurationBuilder.FsyncMode.DEFAULT;

      c = cm.getCacheConfiguration("withouthJmxEnabled");
      assert !c.jmxStatistics().enabled();
      assert gc.globalJmxStatistics().enabled();
      assert gc.globalJmxStatistics().allowDuplicateDomains();
      assertEquals("funky_domain", gc.globalJmxStatistics().domain());
      assert gc.globalJmxStatistics().mbeanServerLookup() instanceof PerThreadMBeanServerLookup;

      c = cm.getCacheConfiguration("dist");
      assert c.clustering().cacheMode() == CacheMode.DIST_SYNC;
      assert c.clustering().l1().lifespan() == 600000;
      assert !deprecated || c.clustering().hash().rehashRpcTimeout() == 120000;
      assert c.clustering().stateTransfer().timeout() == 120000;
      assert c.clustering().l1().cleanupTaskFrequency() == 1200;
      assert c.clustering().hash().consistentHash() == null; // this is just an override.
      assert c.clustering().hash().numOwners() == 3;
      assert c.clustering().l1().enabled();

      c = cm.getCacheConfiguration("dist_with_vnodes");
      assert c.clustering().cacheMode() == CacheMode.DIST_SYNC;
      assert c.clustering().l1().lifespan() == 600000;
      assert !deprecated || c.clustering().hash().rehashRpcTimeout() == 120000;
      assert c.clustering().stateTransfer().timeout() == 120000;
      assert c.clustering().hash().consistentHash() == null; // this is just an override.
      assert c.clustering().hash().numOwners() == 3;
      assert c.clustering().l1().enabled();
      assert c.clustering().hash().numVirtualNodes() == 1;
      if (!deprecated) assert c.clustering().hash().numSegments() == 1000;

      c = cm.getCacheConfiguration("groups");
      assert c.clustering().hash().groups().enabled();
      assert c.clustering().hash().groups().groupers().size() == 1;
      assert c.clustering().hash().groups().groupers().get(0).getKeyType().equals(String.class);

      c = cm.getCacheConfiguration("chunkSize");
      assert c.clustering().stateTransfer().fetchInMemoryState();
      assert c.clustering().stateTransfer().timeout() == 120000;
      assert c.clustering().stateTransfer().chunkSize() == 1000;

      c = cm.getCacheConfiguration("cacheWithCustomInterceptors");
      assert !c.customInterceptors().interceptors().isEmpty();
      assert c.customInterceptors().interceptors().size() == 6;
      for(InterceptorConfiguration i : c.customInterceptors().interceptors()) {
         if (i.interceptor() instanceof FooInterceptor) {
            assert "bar".equals(i.properties().getProperty("foo"));
         }
      }

      c = cm.getCacheConfiguration("evictionCache");
      assert c.eviction().maxEntries() == 5000;
      assert c.eviction().strategy().equals(EvictionStrategy.LRU);
      assert c.expiration().lifespan() == 60000;
      assert c.expiration().maxIdle() == 1000;
      assert c.eviction().threadPolicy() == EvictionThreadPolicy.PIGGYBACK;
      assert c.expiration().wakeUpInterval() == 500;

      c = cm.getCacheConfiguration("withDeadlockDetection");
      assert c.deadlockDetection().enabled();
      assert c.deadlockDetection().spinDuration() == 1221;
      assert c.clustering().cacheMode() == CacheMode.DIST_SYNC;

      c = cm.getCacheConfiguration("storeKeyValueBinary");
      assert c.storeAsBinary().enabled();
      assert c.storeAsBinary().storeKeysAsBinary();
      assert !c.storeAsBinary().storeValuesAsBinary();

      Configuration withJDBCLoader = cm.getCacheConfiguration("withJDBCLoader");
      assert withJDBCLoader.locking().supportsConcurrentUpdates();
   }

   private void assertReaperAndTimeoutInfo(Configuration defaultCfg) {
      assertEquals(123, defaultCfg.transaction().reaperWakeUpInterval());
      assertEquals(3123, defaultCfg.transaction().completedTxTimeout());
   }

}
