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
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
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
            assertEquals(CacheMode.REPL_SYNC, cm.getDefaultCacheConfiguration().clustering().cacheMode());
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

            assertTrue(globalCfg.transport().transport() instanceof JGroupsTransport);
            assertEquals("demoCluster", globalCfg.transport().clusterName());

            Configuration cfg = cm.getDefaultCacheConfiguration();
            assertEquals(CacheMode.REPL_SYNC, cfg.clustering().cacheMode());
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
            assertEquals(10000, cfg.locking().concurrencyLevel());
            assertEquals(IsolationLevel.REPEATABLE_READ, cfg.locking().isolationLevel());
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
            assertTrue(cfg.storeAsBinary().enabled());
            assertTrue(cfg.storeAsBinary().defensive());
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
            assertTrue(cfg.storeAsBinary().enabled());
            assertTrue(!cfg.storeAsBinary().defensive());
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
            assertFalse(cfg.compatibility().enabled());
            assertNull(cfg.compatibility().marshaller());
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
            assertTrue(cfg.compatibility().enabled());
            assertNull(cfg.compatibility().marshaller());
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
            assertTrue(cfg.compatibility().enabled());
            assertTrue(cfg.compatibility().marshaller() instanceof GenericJBossMarshaller);
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
            assertTrue(cfg.dataContainer().<byte[]>keyEquivalence() instanceof ByteArrayEquivalence);
            assertTrue(cfg.dataContainer().valueEquivalence() instanceof AnyEquivalence);
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
            assertTrue(cfg.dataContainer().<byte[]>keyEquivalence() instanceof ByteArrayEquivalence);
            assertTrue(cfg.dataContainer().<byte[]>valueEquivalence() instanceof ByteArrayEquivalence);
         }
      });
   }

   private void assertNamedCacheFile(EmbeddedCacheManager cm, boolean deprecated) {
      final GlobalConfiguration gc = cm.getCacheManagerConfiguration();

      assertTrue(gc.asyncListenerExecutor().factory() instanceof DefaultExecutorFactory);
      assertEquals("5", gc.asyncListenerExecutor().properties().getProperty("maxThreads"));
      if (!deprecated) {
         assertEquals("10000", gc.asyncListenerExecutor().properties().getProperty("queueSize"));
      }
      assertEquals("AsyncListenerThread", gc.asyncListenerExecutor().properties().getProperty("threadNamePrefix"));

      assertTrue(gc.asyncTransportExecutor().factory() instanceof DefaultExecutorFactory);
      // Should be 25, but it's overriden by the test cache manager factory
      assertEquals(String.valueOf(TestCacheManagerFactory.MAX_ASYNC_EXEC_THREADS), gc.asyncTransportExecutor().properties().getProperty("maxThreads"));
      if (!deprecated) {
         assertEquals(String.valueOf(TestCacheManagerFactory.ASYNC_EXEC_QUEUE_SIZE), gc.asyncTransportExecutor().properties().getProperty("queueSize"));
      }
      assertEquals("AsyncSerializationThread", gc.asyncTransportExecutor().properties().getProperty("threadNamePrefix"));

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

      assertTrue(gc.evictionScheduledExecutor().factory() instanceof DefaultScheduledExecutorFactory);
      assertEquals("EvictionThread", gc.evictionScheduledExecutor().properties().getProperty("threadNamePrefix"));

      assertTrue(gc.replicationQueueScheduledExecutor().factory() instanceof DefaultScheduledExecutorFactory);
      assertEquals("ReplicationQueueThread", gc.replicationQueueScheduledExecutor().properties().getProperty("threadNamePrefix"));

      assertTrue(gc.transport().transport() instanceof JGroupsTransport);
      assertEquals("infinispan-cluster", gc.transport().clusterName());
      // Should be "Jalapeno" but it's overriden by the test cache manager factory
      assertTrue(gc.transport().nodeName().contains("Node"));
      assertEquals(50000, gc.transport().distributedSyncTimeout());

      assertEquals(ShutdownHookBehavior.REGISTER, gc.shutdown().hookBehavior());

      assertTrue(gc.serialization().marshaller() instanceof VersionAwareMarshaller);
      assertEquals(Version.getVersionShort("1.0"), gc.serialization().version());
      final Map<Integer, AdvancedExternalizer<?>> externalizers = gc.serialization().advancedExternalizers();
      assertEquals(3, externalizers.size());
      assertTrue(externalizers.get(1234) instanceof AdvancedExternalizerTest.IdViaConfigObj.Externalizer);
      assertTrue(externalizers.get(5678) instanceof AdvancedExternalizerTest.IdViaAnnotationObj.Externalizer);
      assertTrue(externalizers.get(3456) instanceof AdvancedExternalizerTest.IdViaBothObj.Externalizer);

      Configuration defaultCfg = cm.getDefaultCacheConfiguration();

      assertEquals(1000, defaultCfg.locking().lockAcquisitionTimeout());
      assertEquals(100, defaultCfg.locking().concurrencyLevel());
      assertEquals(IsolationLevel.READ_COMMITTED, defaultCfg.locking().isolationLevel());
      if (!deprecated) {
         assertReaperAndTimeoutInfo(defaultCfg);
      }


      Configuration c = cm.getCacheConfiguration("transactional");
      assertTrue(!c.clustering().cacheMode().isClustered());
      assertTrue(c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup);
      assertTrue(c.transaction().useEagerLocking());
      assertTrue(c.transaction().eagerLockingSingleNode());
      assertTrue(!c.transaction().syncRollbackPhase());
      if (!deprecated) {
         assertReaperAndTimeoutInfo(defaultCfg);
      }

      c = cm.getCacheConfiguration("transactional2");
      assertTrue(c.transaction().transactionManagerLookup() instanceof TestLookup);
      assertEquals(10000, c.transaction().cacheStopTimeout());
      assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());
      assertTrue(!c.transaction().autoCommit());

      c = cm.getCacheConfiguration("transactional3");

      if (!deprecated) {
         assertEquals(TransactionProtocol.TOTAL_ORDER, c.transaction().transactionProtocol());
      }

      c = cm.getCacheConfiguration("syncInval");

      assertEquals(CacheMode.INVALIDATION_SYNC, c.clustering().cacheMode());
      assertTrue(!c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().sync().replTimeout());

      c = cm.getCacheConfiguration("asyncInval");

      assertEquals(CacheMode.INVALIDATION_ASYNC, c.clustering().cacheMode());
      assertTrue(!c.clustering().stateTransfer().fetchInMemoryState());
      if (!deprecated) assertTrue(!c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().sync().replTimeout());

      c = cm.getCacheConfiguration("syncRepl");

      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertTrue(!c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().sync().replTimeout());

      c = cm.getCacheConfiguration("asyncRepl");

      assertEquals(CacheMode.REPL_ASYNC, c.clustering().cacheMode());
      assertTrue(!c.clustering().async().useReplQueue());
      assertTrue(!c.clustering().async().asyncMarshalling());
      assertTrue(!c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());

      c = cm.getCacheConfiguration("asyncReplQueue");

      assertEquals(CacheMode.REPL_ASYNC, c.clustering().cacheMode());
      assertTrue(c.clustering().async().useReplQueue());
      assertTrue(!c.clustering().async().asyncMarshalling());
      assertTrue(!c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());

      c = cm.getCacheConfiguration("txSyncRepl");

      assertTrue(c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup);
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertTrue(!c.clustering().stateTransfer().fetchInMemoryState());
      assertTrue(c.clustering().stateTransfer().awaitInitialTransfer());
      assertEquals(15000, c.clustering().sync().replTimeout());

      c = cm.getCacheConfiguration("overriding");

      assertEquals(CacheMode.LOCAL, c.clustering().cacheMode());
      assertEquals(20000, c.locking().lockAcquisitionTimeout());
      assertEquals(1000, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.REPEATABLE_READ, c.locking().isolationLevel());
      assertTrue(!c.storeAsBinary().enabled());

      c = cm.getCacheConfiguration("storeAsBinary");
      assertTrue(c.storeAsBinary().enabled());

      c = cm.getCacheConfiguration("withFileStore");
      assertTrue(c.loaders().preload());
      assertTrue(!c.loaders().passivation());
      assertTrue(!c.loaders().shared());
      assertEquals(1, c.loaders().cacheLoaders().size());

      FileCacheStoreConfiguration loaderCfg = (FileCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);

      assertTrue(loaderCfg.fetchPersistentState());
      assertTrue(loaderCfg.ignoreModifications());
      assertTrue(loaderCfg.purgeOnStartup());
      assertEquals("/tmp/FileCacheStore-Location", loaderCfg.location());
      assertEquals(FileCacheStoreConfigurationBuilder.FsyncMode.PERIODIC, loaderCfg.fsyncMode());
      assertEquals(2000, loaderCfg.fsyncInterval());
      assertEquals(20000, loaderCfg.singletonStore().pushStateTimeout());
      assertTrue(loaderCfg.singletonStore().pushStateWhenCoordinator());
      assertEquals(5, loaderCfg.async().threadPoolSize());
      assertEquals(15000, loaderCfg.async().flushLockTimeout());
      assertTrue(loaderCfg.async().enabled());
      assertEquals(700, loaderCfg.async().modificationQueueSize());

      c = cm.getCacheConfiguration("withClusterLoader");
      assertEquals(1, c.loaders().cacheLoaders().size());
      ClusterCacheLoaderConfiguration clusterLoaderCfg = (ClusterCacheLoaderConfiguration) c.loaders().cacheLoaders().get(0);
      assertEquals(15000, clusterLoaderCfg.remoteCallTimeout());

      c = cm.getCacheConfiguration("withLoaderDefaults");
      loaderCfg = (FileCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);
      assertEquals("/tmp/Another-FileCacheStore-Location", loaderCfg.location());
      assertEquals(FileCacheStoreConfigurationBuilder.FsyncMode.DEFAULT, loaderCfg.fsyncMode());

      c = cm.getCacheConfiguration("withouthJmxEnabled");
      assertTrue(!c.jmxStatistics().enabled());
      assertTrue(gc.globalJmxStatistics().enabled());
      assertTrue(gc.globalJmxStatistics().allowDuplicateDomains());
      assertEquals("funky_domain", gc.globalJmxStatistics().domain());
      assertTrue(gc.globalJmxStatistics().mbeanServerLookup() instanceof PerThreadMBeanServerLookup);

      c = cm.getCacheConfiguration("dist");
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertEquals(600000, c.clustering().l1().lifespan());
      if (deprecated) assertEquals(120000, c.clustering().hash().rehashRpcTimeout());
      assertEquals(120000, c.clustering().stateTransfer().timeout());
      assertEquals(1200, c.clustering().l1().cleanupTaskFrequency());
      assertEquals(null, c.clustering().hash().consistentHash()); // this is just an override.
      assertEquals(3, c.clustering().hash().numOwners());
      assertTrue(c.clustering().l1().enabled());

      c = cm.getCacheConfiguration("dist_with_vnodes");
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertEquals(600000, c.clustering().l1().lifespan());
      if (deprecated) assertEquals(120000, c.clustering().hash().rehashRpcTimeout());
      assertEquals(120000, c.clustering().stateTransfer().timeout());
      assertEquals(null, c.clustering().hash().consistentHash()); // this is just an override.
      assertEquals(3, c.clustering().hash().numOwners());
      assertTrue(c.clustering().l1().enabled());
      assertEquals(1, c.clustering().hash().numVirtualNodes());
      if (!deprecated) assertEquals(1000, c.clustering().hash().numSegments());

      c = cm.getCacheConfiguration("groups");
      assertTrue(c.clustering().hash().groups().enabled());
      assertEquals(1, c.clustering().hash().groups().groupers().size());
      assertEquals(String.class, c.clustering().hash().groups().groupers().get(0).getKeyType());

      c = cm.getCacheConfiguration("chunkSize");
      assertTrue(c.clustering().stateTransfer().fetchInMemoryState());
      assertEquals(120000, c.clustering().stateTransfer().timeout());
      assertEquals(1000, c.clustering().stateTransfer().chunkSize());

      c = cm.getCacheConfiguration("cacheWithCustomInterceptors");
      assertTrue(!c.customInterceptors().interceptors().isEmpty());
      assertEquals(6, c.customInterceptors().interceptors().size());
      for(InterceptorConfiguration i : c.customInterceptors().interceptors()) {
         if (i.interceptor() instanceof FooInterceptor) {
            assertEquals(i.properties().getProperty("foo"), "bar");
         }
      }

      c = cm.getCacheConfiguration("evictionCache");
      assertEquals(5000, c.eviction().maxEntries());
      assertEquals(EvictionStrategy.LRU, c.eviction().strategy());
      assertEquals(60000, c.expiration().lifespan());
      assertEquals(1000, c.expiration().maxIdle());
      assertEquals(EvictionThreadPolicy.PIGGYBACK, c.eviction().threadPolicy());
      assertEquals(500, c.expiration().wakeUpInterval());

      c = cm.getCacheConfiguration("withDeadlockDetection");
      assertTrue(c.deadlockDetection().enabled());
      assertEquals(1221, c.deadlockDetection().spinDuration());
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());

      c = cm.getCacheConfiguration("storeKeyValueBinary");
      assertTrue(c.storeAsBinary().enabled());
      assertTrue(c.storeAsBinary().storeKeysAsBinary());
      assertTrue(!c.storeAsBinary().storeValuesAsBinary());

      Configuration withJDBCLoader = cm.getCacheConfiguration("withJDBCLoader");
      assertTrue(withJDBCLoader.locking().supportsConcurrentUpdates());
   }

   private void assertReaperAndTimeoutInfo(Configuration defaultCfg) {
      assertEquals(123, defaultCfg.transaction().reaperWakeUpInterval());
      assertEquals(3123, defaultCfg.transaction().completedTxTimeout());
   }

}
