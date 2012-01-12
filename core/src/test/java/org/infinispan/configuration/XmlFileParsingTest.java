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

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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

//      assert gc.getEvictionScheduledExecutorFactoryClass().equals("org.infinispan.executors.DefaultScheduledExecutorFactory");
//      assert gc.getEvictionScheduledExecutorProperties().getProperty("threadNamePrefix").equals("EvictionThread");
//
//      assert gc.getReplicationQueueScheduledExecutorFactoryClass().equals("org.infinispan.executors.DefaultScheduledExecutorFactory");
//      assert gc.getReplicationQueueScheduledExecutorProperties().getProperty("threadNamePrefix").equals("ReplicationQueueThread");
//
//      assert gc.getTransportClass().equals("org.infinispan.remoting.transport.jgroups.JGroupsTransport");
//      assert gc.getClusterName().equals("infinispan-cluster");
//      assert gc.getTransportNodeName().equals("Jalapeno");
//      assert gc.getDistributedSyncTimeout() == 50000;
//
//      assert gc.getShutdownHookBehavior().equals(ShutdownHookBehavior.REGISTER);
//
//      assert gc.getMarshallerClass().equals("org.infinispan.marshall.VersionAwareMarshaller");
//      assert gc.getMarshallVersionString().equals("1.0");
//      List<AdvancedExternalizerConfig> advancedExternalizers = gc.getExternalizers();
//      assert advancedExternalizers.size() == 3;
//      AdvancedExternalizerConfig advancedExternalizer = advancedExternalizers.get(0);
//      assert advancedExternalizer.getId() == 1234;
//      assert advancedExternalizer.getExternalizerClass().equals("org.infinispan.marshall.AdvancedExternalizerTest$IdViaConfigObj$Externalizer");
//      advancedExternalizer = advancedExternalizers.get(1);
//      assert advancedExternalizer.getExternalizerClass().equals("org.infinispan.marshall.AdvancedExternalizerTest$IdViaAnnotationObj$Externalizer");
//      advancedExternalizer = advancedExternalizers.get(2);
//      assert advancedExternalizer.getId() == 3456;
//      assert advancedExternalizer.getExternalizerClass().equals("org.infinispan.marshall.AdvancedExternalizerTest$IdViaBothObj$Externalizer");
//
//      Configuration defaultCfg = parser.parseDefaultConfiguration();
//
//      assert defaultCfg.getLockAcquisitionTimeout() == 1000;
//      assert defaultCfg.getConcurrencyLevel() == 100;
//      assert defaultCfg.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
//
//      Map<String, Configuration> namedCaches = parser.parseNamedConfigurations();
//
//      Configuration c = getNamedCacheConfig(namedCaches, "transactional", defaultCfg);
//
//      assert !c.getCacheMode().isClustered();
//      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
//      assert c.isUseEagerLocking();
//      assert c.isEagerLockSingleNode();
//      assert !c.isSyncRollbackPhase();
//
//      c = getNamedCacheConfig(namedCaches, "transactional2", defaultCfg);
//      assert c.getTransactionManagerLookupClass().equals("org.something.Lookup");
//      assert c.getCacheStopTimeout() == 10000;
//      assert c.getTransactionLockingMode().equals(LockingMode.PESSIMISTIC);
//      assert !c.isTransactionAutoCommit();
//
//      c = getNamedCacheConfig(namedCaches, "syncRepl", defaultCfg);
//
//      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
//      assert !c.isFetchInMemoryState();
//      assert c.getSyncReplTimeout() == 15000;
//
//      c = getNamedCacheConfig(namedCaches, "asyncRepl", defaultCfg);
//
//      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
//      assert !c.isUseReplQueue();
//      assert !c.isUseAsyncMarshalling();
//      assert !c.isFetchInMemoryState();
//
//      c = getNamedCacheConfig(namedCaches, "asyncReplQueue", defaultCfg);
//
//      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
//      assert c.isUseReplQueue();
//      assert !c.isUseAsyncMarshalling();
//      assert !c.isFetchInMemoryState();
//
//      c = getNamedCacheConfig(namedCaches, "txSyncRepl", defaultCfg);
//
//      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
//      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
//      assert !c.isFetchInMemoryState();
//      assert c.getSyncReplTimeout() == 15000;
//
//      c = getNamedCacheConfig(namedCaches, "overriding", defaultCfg);
//
//      assert c.getCacheMode() == Configuration.CacheMode.LOCAL;
//      assert c.getLockAcquisitionTimeout() == 20000;
//      assert c.getConcurrencyLevel() == 1000;
//      assert c.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;
//      assert !c.isStoreAsBinary();
//
//      c = getNamedCacheConfig(namedCaches, "storeAsBinary", defaultCfg);
//      assert c.isStoreAsBinary();
//
//      c = getNamedCacheConfig(namedCaches, "withLoader", defaultCfg);
//      CacheLoaderManagerConfig loaderManagerConfig = c.getCacheLoaderManagerConfig();
//      assert loaderManagerConfig.isPreload();
//      assert !loaderManagerConfig.isPassivation();
//      assert !loaderManagerConfig.isShared();
//      assert loaderManagerConfig.getCacheLoaderConfigs().size() == 1;
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
//      c = getNamedCacheConfig(namedCaches, "withLoaderDefaults", defaultCfg);
//      csConf = (FileCacheStoreConfig) c.getCacheLoaders().get(0);
//      assert csConf.getCacheLoaderClassName().equals("org.infinispan.loaders.file.FileCacheStore");
//      assert csConf.getLocation().equals("/tmp/Another-FileCacheStore-Location");
//      assert csConf.getFsyncMode() == FileCacheStoreConfig.FsyncMode.DEFAULT;
//
//      c = getNamedCacheConfig(namedCaches, "withouthJmxEnabled", defaultCfg);
//      assert !c.isExposeJmxStatistics();
//      assert !gc.isExposeGlobalJmxStatistics();
//      assert gc.isAllowDuplicateDomains();
//      assert gc.getJmxDomain().equals("funky_domain");
//      assert gc.getMBeanServerLookup().equals("org.infinispan.jmx.PerThreadMBeanServerLookup");
//
//      c = getNamedCacheConfig(namedCaches, "dist", defaultCfg);
//      assert c.getCacheMode() == Configuration.CacheMode.DIST_SYNC;
//      assert c.getL1Lifespan() == 600000;
//      assert c.getRehashWaitTime() == 120000;
//      assert c.getConsistentHashClass().equals(TopologyAwareConsistentHash.class.getName());
//      assert c.getNumOwners() == 3;
//      assert c.isL1CacheEnabled();
//
//      c = getNamedCacheConfig(namedCaches, "groups", defaultCfg);
//      Assert.assertTrue(c.isGroupsEnabled());
//      Assert.assertEquals(c.getGroupers().size(), 1);
//      Assert.assertEquals(c.getGroupers().get(0).getKeyType(), String.class);
//
//      c = getNamedCacheConfig(namedCaches, "cacheWithCustomInterceptors", defaultCfg);
//      assert !c.getCustomInterceptors().isEmpty();
//      assert c.getCustomInterceptors().size() == 5;
//
//      c = getNamedCacheConfig(namedCaches, "evictionCache", defaultCfg);
//      assert c.getEvictionMaxEntries() == 5000;
//      assert c.getEvictionStrategy().equals(EvictionStrategy.FIFO);
//      assert c.getExpirationLifespan() == 60000;
//      assert c.getExpirationMaxIdle() == 1000;
//      assert c.getEvictionThreadPolicy() == EvictionThreadPolicy.PIGGYBACK;
//      assert c.getExpirationWakeUpInterval() == 500;
//
//      c = getNamedCacheConfig(namedCaches, "withDeadlockDetection", defaultCfg);
//      assert c.isEnableDeadlockDetection();
//      assert c.getDeadlockDetectionSpinDuration() == 1221;
//      assert c.getCacheMode() == Configuration.CacheMode.DIST_SYNC;
   }

}