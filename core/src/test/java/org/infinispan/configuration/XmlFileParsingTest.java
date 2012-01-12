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

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.infinispan.Version;
import org.infinispan.configuration.cache.AbstractLoaderConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.AdvancedExternalizerTest;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.something.Lookup;
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
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromStream(is);
      Assert.assertEquals(cm.getDefaultCacheConfiguration().clustering().cacheMode(), CacheMode.REPL_SYNC);
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
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromXml("configs/named-cache-test.xml");
      assertNamedCacheFile(cm);
   }

   private void assertNamedCacheFile(EmbeddedCacheManager cm) {
      final GlobalConfiguration gc = cm.getCacheManagerConfiguration();

      assert gc.asyncListenerExecutor().factory() instanceof DefaultExecutorFactory;
      assert gc.asyncListenerExecutor().properties().getProperty("maxThreads").equals("5");
      assert gc.asyncListenerExecutor().properties().getProperty("threadNamePrefix").equals("AsyncListenerThread");

      assert gc.asyncTransportExecutor().factory() instanceof DefaultExecutorFactory;
      assert gc.asyncTransportExecutor().properties().getProperty("maxThreads").equals("25");
      assert gc.asyncTransportExecutor().properties().getProperty("threadNamePrefix").equals("AsyncSerializationThread");

      assert gc.evictionScheduledExecutor().factory() instanceof DefaultScheduledExecutorFactory;
      assert gc.evictionScheduledExecutor().properties().getProperty("threadNamePrefix").equals("EvictionThread");

      assert gc.replicationQueueScheduledExecutor().factory() instanceof DefaultScheduledExecutorFactory;
      assert gc.replicationQueueScheduledExecutor().properties().getProperty("threadNamePrefix").equals("ReplicationQueueThread");

      assert gc.transport().transport() instanceof JGroupsTransport;
      assert gc.transport().clusterName().equals("infinispan-cluster");
      assert gc.transport().nodeName().equals("Jalapeno");
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

      Configuration c = cm.getCacheConfiguration("transactional");
      assert !c.clustering().cacheMode().isClustered();
      assert c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup;
      assert c.transaction().useEagerLocking();
      assert c.transaction().eagerLockingSingleNode();
      assert !c.transaction().syncRollbackPhase();

      c = cm.getCacheConfiguration("transactional2");
      assert c.transaction().transactionManagerLookup() instanceof Lookup;
      assert c.transaction().cacheStopTimeout() == 10000;
      assert c.transaction().lockingMode().equals(LockingMode.PESSIMISTIC);
      assert !c.transaction().autoCommit();

      c = cm.getCacheConfiguration("syncRepl");

      assert c.clustering().cacheMode() == CacheMode.REPL_SYNC;
      assert !c.clustering().stateRetrieval().fetchInMemoryState();
      assert c.clustering().sync().replTimeout() == 15000;

      c = cm.getCacheConfiguration("asyncRepl");

      assert c.clustering().cacheMode() == CacheMode.REPL_ASYNC;
      assert !c.clustering().async().useReplQueue();
      assert !c.clustering().async().asyncMarshalling();
      assert !c.clustering().stateRetrieval().fetchInMemoryState();

      c = cm.getCacheConfiguration("asyncReplQueue");

      assert c.clustering().cacheMode() == CacheMode.REPL_ASYNC;
      assert c.clustering().async().useReplQueue();
      assert !c.clustering().async().asyncMarshalling();
      assert !c.clustering().stateRetrieval().fetchInMemoryState();

      c = cm.getCacheConfiguration("txSyncRepl");

      assert c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup;
      assert c.clustering().cacheMode() == CacheMode.REPL_SYNC;
      assert !c.clustering().stateRetrieval().fetchInMemoryState();
      assert c.clustering().sync().replTimeout() == 15000;

      c = cm.getCacheConfiguration("overriding");

      assert c.clustering().cacheMode() == CacheMode.LOCAL;
      assert c.locking().lockAcquisitionTimeout() == 20000;
      assert c.locking().concurrencyLevel() == 1000;
      assert c.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      assert !c.storeAsBinary().enabled();

      c = cm.getCacheConfiguration("storeAsBinary");
      assert c.storeAsBinary().enabled();

      c = cm.getCacheConfiguration("withLoader");
      assert c.loaders().preload();
      assert !c.loaders().passivation();
      assert !c.loaders().shared();
      assert c.loaders().cacheLoaders().size() == 1;

//      FileCacheStoreConfig csConf = (FileCacheStoreConfig) loaderManagerConfig.getFirstCacheLoaderConfig();
//      assert csConf.getCacheLoaderClassName().equals("org.infinispan.loaders.file.FileCacheStore");
//      assert csConf.isFetchPersistentState();
//      assert csConf.isIgnoreModifications();
//      assert csConf.isPurgeOnStartup();
//      assert csConf.getLocation().equals("/tmp/FileCacheStore-Location");
//      assert csConf.getFsyncMode() == FileCacheStoreConfig.FsyncMode.PERIODIC;
//      assert csConf.getFsyncInterval() == 2000;
//      assert csConf.getSingletonStoreConfig().getPushStateTimeout() == 20000;
//      assert csConf.getSingletonStoreConfig().isPushStateWhenCoordinator();
//      assert csConf.getAsyncStoreConfig().getThreadPoolSize() == 5;
//      assert csConf.getAsyncStoreConfig().getFlushLockTimeout() == 15000;
//      assert csConf.getAsyncStoreConfig().isEnabled();
//      assert csConf.getAsyncStoreConfig().getModificationQueueSize() == 700;
//
//      c = cm.getCacheConfiguration("withLoaderDefaults", defaultCfg);
//      csConf = (FileCacheStoreConfig) c.getCacheLoaders().get(0);
//      assert csConf.getCacheLoaderClassName().equals("org.infinispan.loaders.file.FileCacheStore");
//      assert csConf.getLocation().equals("/tmp/Another-FileCacheStore-Location");
//      assert csConf.getFsyncMode() == FileCacheStoreConfig.FsyncMode.DEFAULT;
//
//      c = cm.getCacheConfiguration("withouthJmxEnabled", defaultCfg);
//      assert !c.isExposeJmxStatistics();
//      assert !gc.isExposeGlobalJmxStatistics();
//      assert gc.isAllowDuplicateDomains();
//      assert gc.getJmxDomain().equals("funky_domain");
//      assert gc.getMBeanServerLookup().equals("org.infinispan.jmx.PerThreadMBeanServerLookup");
//
//      c = cm.getCacheConfiguration("dist", defaultCfg);
//      assert c.getCacheMode() == Configuration.CacheMode.DIST_SYNC;
//      assert c.getL1Lifespan() == 600000;
//      assert c.getRehashWaitTime() == 120000;
//      assert c.getConsistentHashClass().equals(TopologyAwareConsistentHash.class.getName());
//      assert c.getNumOwners() == 3;
//      assert c.isL1CacheEnabled();
//
//      c = cm.getCacheConfiguration("groups", defaultCfg);
//      Assert.assertTrue(c.isGroupsEnabled());
//      Assert.assertEquals(c.getGroupers().size(), 1);
//      Assert.assertEquals(c.getGroupers().get(0).getKeyType(), String.class);
//
//      c = cm.getCacheConfiguration("cacheWithCustomInterceptors", defaultCfg);
//      assert !c.getCustomInterceptors().isEmpty();
//      assert c.getCustomInterceptors().size() == 5;
//
//      c = cm.getCacheConfiguration("evictionCache", defaultCfg);
//      assert c.getEvictionMaxEntries() == 5000;
//      assert c.getEvictionStrategy().equals(EvictionStrategy.FIFO);
//      assert c.getExpirationLifespan() == 60000;
//      assert c.getExpirationMaxIdle() == 1000;
//      assert c.getEvictionThreadPolicy() == EvictionThreadPolicy.PIGGYBACK;
//      assert c.getExpirationWakeUpInterval() == 500;
//
//      c = cm.getCacheConfiguration("withDeadlockDetection", defaultCfg);
//      assert c.isEnableDeadlockDetection();
//      assert c.getDeadlockDetectionSpinDuration() == 1221;
//      assert c.getCacheMode() == Configuration.CacheMode.DIST_SYNC;
   }

}