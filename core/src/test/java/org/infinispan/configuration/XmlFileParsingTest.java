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

import org.infinispan.Version;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.FileCacheStoreConfiguration;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.AdvancedExternalizerTest;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.tx.TestLookup;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.infinispan.test.TestingUtil.*;

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
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromStream(is);
      GlobalConfiguration globalCfg = cm.getCacheManagerConfiguration();

      assert globalCfg.transport().transport() instanceof JGroupsTransport;
      assert globalCfg.transport().clusterName().equals("demoCluster");

      Configuration cfg = cm.getDefaultCacheConfiguration();
      assert cfg.clustering().cacheMode() == CacheMode.REPL_SYNC;
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
      TestCacheManagerFactory.fromStream(is);
   }

   public void testNoSchemaWithStuff() throws IOException {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "    <default>\n" +
            "        <locking concurrencyLevel=\"10000\" isolationLevel=\"REPEATABLE_READ\" />\n" +
            "    </default>\n" +
            INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromStream(is);

      Configuration cfg = cm.getDefaultCacheConfiguration();
      assert cfg.locking().concurrencyLevel() == 10000;
      assert cfg.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
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
      assert c.transaction().transactionManagerLookup() instanceof TestLookup;
      assert c.transaction().cacheStopTimeout() == 10000;
      assert c.transaction().lockingMode().equals(LockingMode.PESSIMISTIC);
      assert !c.transaction().autoCommit();

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

      c = cm.getCacheConfiguration("withLoader");
      assert c.loaders().preload();
      assert !c.loaders().passivation();
      assert !c.loaders().shared();
      assert c.loaders().cacheLoaders().size() == 1;

      FileCacheStoreConfiguration loaderCfg = (FileCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);

      assert loaderCfg.fetchPersistentState();
      assert loaderCfg.ignoreModifications();
      assert loaderCfg.purgeOnStartup();
      assert loaderCfg.location().equals("/tmp/FileCacheStore-Location");
      assert loaderCfg.fsyncMode() == FileCacheStoreConfigurationBuilder.FsyncMode.PERIODIC;
      assert loaderCfg.fsyncInterval() == 2000;
      assert loaderCfg.singletonStore().pushStateTimeout() == 20000;
      assert loaderCfg.singletonStore().pushStateWhenCoordinator();
      assert loaderCfg.async().threadPoolSize() == 5;
      assert loaderCfg.async().flushLockTimeout() == 15000;
      assert loaderCfg.async().enabled();
      assert loaderCfg.async().modificationQueueSize() == 700;

      c = cm.getCacheConfiguration("withLoaderDefaults");
      loaderCfg = (FileCacheStoreConfiguration) c.loaders().cacheLoaders().get(0);
      assert loaderCfg.location().equals("/tmp/Another-FileCacheStore-Location");
      assert loaderCfg.fsyncMode() == FileCacheStoreConfigurationBuilder.FsyncMode.DEFAULT;

      c = cm.getCacheConfiguration("withouthJmxEnabled");
      assert !c.jmxStatistics().enabled();
      assert gc.globalJmxStatistics().enabled();
      assert gc.globalJmxStatistics().allowDuplicateDomains();
      assert gc.globalJmxStatistics().domain().equals("funky_domain");
      assert gc.globalJmxStatistics().mbeanServerLookup() instanceof PerThreadMBeanServerLookup;

      c = cm.getCacheConfiguration("dist");
      assert c.clustering().cacheMode() == CacheMode.DIST_SYNC;
      assert c.clustering().l1().lifespan() == 600000;
      assert c.clustering().hash().rehashRpcTimeout() == 120000;
      assert c.clustering().stateTransfer().timeout() == 120000;
      assert c.clustering().hash().consistentHash() == null; // this is just an override.
      assert c.clustering().hash().numOwners() == 3;
      assert c.clustering().l1().enabled();

      c = cm.getCacheConfiguration("dist_with_vnodes");
      assert c.clustering().cacheMode() == CacheMode.DIST_SYNC;
      assert c.clustering().l1().lifespan() == 600000;
      assert c.clustering().hash().rehashRpcTimeout() == 120000;
      assert c.clustering().stateTransfer().timeout() == 120000;
      assert c.clustering().hash().consistentHash() == null; // this is just an override.
      assert c.clustering().hash().numOwners() == 3;
      assert c.clustering().l1().enabled();
      assert c.clustering().hash().numVirtualNodes() == 1000;

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
      assert c.customInterceptors().interceptors().size() == 5;

      c = cm.getCacheConfiguration("evictionCache");
      assert c.eviction().maxEntries() == 5000;
      assert c.eviction().strategy().equals(EvictionStrategy.LIRS);
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
   }

}