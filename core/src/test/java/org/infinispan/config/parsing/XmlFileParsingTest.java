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
package org.infinispan.config.parsing;

import org.infinispan.Version;
import org.infinispan.config.*;
import org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior;
import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.infinispan.test.TestingUtil.*;

@Test(groups = "unit", testName = "config.parsing.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   public void testNamedCacheFileJaxb() throws Exception {
      String schemaFileName = String.format("infinispan-config-%s.xsd", Version.MAJOR_MINOR);
      testNamedCacheFile(InfinispanConfiguration.newInfinispanConfiguration(
            "configs/named-cache-test.xml", "schema/" + schemaFileName, new ConfigurationValidatingVisitor(), Thread.currentThread().getContextClassLoader()));
   }
   
   public void testNamedCacheFileWithAllValidators() throws Exception {
      String schemaFileName = String.format("infinispan-config-%s.xsd", Version.MAJOR_MINOR);
      testNamedCacheFile(InfinispanConfiguration.newInfinispanConfiguration(
               "configs/named-cache-test.xml", "schema/" + schemaFileName,
               new DelegatingConfigurationVisitor(new ConfigurationBeanVisitor[] {
                        new ConfigurationValidatingVisitor(),
                        new TimeoutConfigurationValidatingVisitor()}), Thread.currentThread().getContextClassLoader()));
   }

   public void testConfigurationMergingJaxb() throws Exception {
      testConfigurationMerging(InfinispanConfiguration
            .newInfinispanConfiguration("configs/named-cache-test.xml", Thread.currentThread().getContextClassLoader()));
   }

   public void testConfigSampleAllValidation() throws Exception {
      String schemaFileName = String.format("infinispan-config-%s.xsd", Version.MAJOR_MINOR);
      InfinispanConfiguration.newInfinispanConfiguration("config-samples/sample.xml", "schema/" + schemaFileName, new ConfigurationValidatingVisitor(), Thread.currentThread().getContextClassLoader());
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
      InfinispanConfiguration c = InfinispanConfiguration.newInfinispanConfiguration(is);
      GlobalConfiguration gc = c.parseGlobalConfiguration();
      assert gc.getTransportClass().equals(JGroupsTransport.class.getName());
      assert gc.getClusterName().equals("demoCluster");

      Configuration def = c.parseDefaultConfiguration();
      assert def.getCacheMode() == Configuration.CacheMode.REPL_SYNC;

      Map<String, Configuration> named = c.parseNamedConfigurations();
      assert named != null;
      assert named.isEmpty();
   }
   
   public void testBackwardCompatibleInputCacheConfiguration() throws Exception {
      
      //read 4.0 configuration file against 4.1 schema
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

      String schemaFileName = String.format("infinispan-config-%s.xsd", Version.MAJOR_MINOR);
      
      InputStream is = new ByteArrayInputStream(config.getBytes());      
      InfinispanConfiguration c = InfinispanConfiguration.newInfinispanConfiguration(is,
               InfinispanConfiguration.findSchemaInputStream(schemaFileName));
      GlobalConfiguration gc = c.parseGlobalConfiguration();
      
      assert gc.getTransportClass().equals(JGroupsTransport.class.getName());
      assert gc.getClusterName().equals("demoCluster");

      Configuration def = c.parseDefaultConfiguration();
      assert def.getCacheMode() == Configuration.CacheMode.REPL_SYNC;

      Map<String, Configuration> named = c.parseNamedConfigurations();
      assert named != null;
      assert named.isEmpty();
   }

   public void testStoreAsBinary() throws IOException {
      String xml = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>" +
               "<storeAsBinary storeKeysAsBinary=\"true\" storeValuesAsBinary=\"false\" enabled=\"true\" />" +
            "</default>" +
            INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(xml.getBytes());
      InfinispanConfiguration c = InfinispanConfiguration.newInfinispanConfiguration(is);
      GlobalConfiguration gc = c.parseGlobalConfiguration();
      Configuration defaultCfg = c.parseDefaultConfiguration();
      assert defaultCfg.isStoreAsBinary();
      assert defaultCfg.isStoreKeysAsBinary();
      assert !defaultCfg.isStoreValuesAsBinary();
   }

   public void testCustomInterceptorsInNamedCache() throws IOException {
      String xml = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default />" +
            "<namedCache name=\"x\">" +
            "<customInterceptors>\n" +
            "         <interceptor position=\"first\" class=\""+CustomInterceptor1.class.getName()+"\" />" +
            "         <interceptor" +
            "            position=\"last\"" +
            "            class=\""+CustomInterceptor2.class.getName()+"\"" +
            "         />" +
            "</customInterceptors>" +
            "</namedCache>" +
            INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(xml.getBytes());
      InfinispanConfiguration c = InfinispanConfiguration.newInfinispanConfiguration(is);
      GlobalConfiguration gc = c.parseGlobalConfiguration();
      Map<String, Configuration> named = c.parseNamedConfigurations();
      Configuration cfg = named.get("x");
      List<CustomInterceptorConfig> ci = cfg.getCustomInterceptors();
      assert ci.size() == 2;
      assert ci.get(0).isFirst();
      assert ci.get(0).getClassName().equals(CustomInterceptor1.class.getName());
      assert ci.get(1).isLast();
      assert ci.get(1).getClassName().equals(CustomInterceptor2.class.getName());
   }

   public static final class CustomInterceptor1 extends CommandInterceptor {}
   public static final class CustomInterceptor2 extends CommandInterceptor {}

   public void testNoSchemaWithStuff() throws IOException {
      String xml = INFINISPAN_START_TAG_NO_SCHEMA +
              "    <default>\n" +
              "        <locking concurrencyLevel=\"10000\" isolationLevel=\"REPEATABLE_READ\" />\n" +
              "    </default>\n" +
              INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(xml.getBytes());
      InfinispanConfiguration c = InfinispanConfiguration.newInfinispanConfiguration(is);
      GlobalConfiguration gc = c.parseGlobalConfiguration();
      Configuration def = c.parseDefaultConfiguration();
      assert def.getConcurrencyLevel() == 10000;
      assert def.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;
   }

   private void testNamedCacheFile(XmlConfigurationParser parser) {

      GlobalConfiguration gc = parser.parseGlobalConfiguration();

      assert gc.getAsyncListenerExecutorFactoryClass().equals("org.infinispan.executors.DefaultExecutorFactory");
      assert gc.getAsyncListenerExecutorProperties().getProperty("maxThreads").equals("5");
      assert gc.getAsyncListenerExecutorProperties().getProperty("threadNamePrefix").equals("AsyncListenerThread");

      assert gc.getAsyncTransportExecutorFactoryClass().equals("org.infinispan.executors.DefaultExecutorFactory");
      assert gc.getAsyncTransportExecutorProperties().getProperty("maxThreads").equals("25");
      assert gc.getAsyncTransportExecutorProperties().getProperty("threadNamePrefix").equals("AsyncSerializationThread");

      assert gc.getEvictionScheduledExecutorFactoryClass().equals("org.infinispan.executors.DefaultScheduledExecutorFactory");
      assert gc.getEvictionScheduledExecutorProperties().getProperty("threadNamePrefix").equals("EvictionThread");

      assert gc.getReplicationQueueScheduledExecutorFactoryClass().equals("org.infinispan.executors.DefaultScheduledExecutorFactory");
      assert gc.getReplicationQueueScheduledExecutorProperties().getProperty("threadNamePrefix").equals("ReplicationQueueThread");

      assert gc.getTransportClass().equals("org.infinispan.remoting.transport.jgroups.JGroupsTransport");
      assert gc.getClusterName().equals("infinispan-cluster");
      assert gc.getTransportNodeName().equals("Jalapeno");
      assert gc.getDistributedSyncTimeout() == 50000;

      assert gc.getShutdownHookBehavior().equals(ShutdownHookBehavior.REGISTER);

      assert gc.getMarshallerClass().equals("org.infinispan.marshall.VersionAwareMarshaller");
      assert gc.getMarshallVersionString().equals("1.0");
      List<AdvancedExternalizerConfig> advancedExternalizers = gc.getExternalizers();
      assert advancedExternalizers.size() == 3;
      AdvancedExternalizerConfig advancedExternalizer = advancedExternalizers.get(0);
      assert advancedExternalizer.getId() == 1234;
      assert advancedExternalizer.getExternalizerClass().equals("org.infinispan.marshall.AdvancedExternalizerTest$IdViaConfigObj$Externalizer");
      advancedExternalizer = advancedExternalizers.get(1);
      assert advancedExternalizer.getExternalizerClass().equals("org.infinispan.marshall.AdvancedExternalizerTest$IdViaAnnotationObj$Externalizer");
      advancedExternalizer = advancedExternalizers.get(2);
      assert advancedExternalizer.getId() == 3456;
      assert advancedExternalizer.getExternalizerClass().equals("org.infinispan.marshall.AdvancedExternalizerTest$IdViaBothObj$Externalizer");

      Configuration defaultConfiguration = parser.parseDefaultConfiguration();

      assert defaultConfiguration.getLockAcquisitionTimeout() == 1000;
      assert defaultConfiguration.getConcurrencyLevel() == 100;
      assert defaultConfiguration.getIsolationLevel() == IsolationLevel.READ_COMMITTED;

      Map<String, Configuration> namedCaches = parser.parseNamedConfigurations();

      Configuration c = getNamedCacheConfig(namedCaches, "transactional");

      assert !c.getCacheMode().isClustered();
      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
      assert c.isUseEagerLocking();
      assert c.isEagerLockSingleNode();
      assert !c.isSyncRollbackPhase();

      c = getNamedCacheConfig(namedCaches, "transactional2");
      assert c.getTransactionManagerLookupClass().equals("org.something.Lookup");
      assert c.getCacheStopTimeout() == 10000;

      c = getNamedCacheConfig(namedCaches, "syncRepl");

      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
      assert !c.isFetchInMemoryState();
      assert c.getSyncReplTimeout() == 15000;

      c = getNamedCacheConfig(namedCaches, "asyncRepl");

      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert !c.isUseReplQueue();
      assert !c.isUseAsyncMarshalling();
      assert !c.isFetchInMemoryState();

      c = getNamedCacheConfig(namedCaches, "asyncReplQueue");

      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert c.isUseReplQueue();
      assert !c.isUseAsyncMarshalling();
      assert !c.isFetchInMemoryState();

      c = getNamedCacheConfig(namedCaches, "txSyncRepl");

      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
      assert !c.isFetchInMemoryState();
      assert c.getSyncReplTimeout() == 15000;

      c = getNamedCacheConfig(namedCaches, "overriding");

      assert c.getTransactionManagerLookupClass() == null;
      assert c.getCacheMode() == Configuration.CacheMode.LOCAL;
      assert c.getLockAcquisitionTimeout() == 20000;
      assert c.getConcurrencyLevel() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;
      assert !c.isStoreAsBinary();

      c = getNamedCacheConfig(namedCaches, "storeAsBinary");
      assert c.isStoreAsBinary();

      c = getNamedCacheConfig(namedCaches, "withLoader");
      CacheLoaderManagerConfig loaderManagerConfig = c.getCacheLoaderManagerConfig();
      assert loaderManagerConfig.isPreload();
      assert !loaderManagerConfig.isPassivation();
      assert !loaderManagerConfig.isShared();
      assert loaderManagerConfig.getCacheLoaderConfigs().size() == 1;
      FileCacheStoreConfig csConf = (FileCacheStoreConfig) loaderManagerConfig.getFirstCacheLoaderConfig();
      assert csConf.getCacheLoaderClassName().equals("org.infinispan.loaders.file.FileCacheStore");
      assert csConf.isFetchPersistentState();
      assert csConf.isIgnoreModifications();
      assert csConf.isPurgeOnStartup();
      assert csConf.getLocation().equals("/tmp/FileCacheStore-Location");
      assert csConf.getFsyncMode() == FileCacheStoreConfig.FsyncMode.PERIODIC;
      assert csConf.getFsyncInterval() == 2000;
      assert csConf.getSingletonStoreConfig().getPushStateTimeout() == 20000;
      assert csConf.getSingletonStoreConfig().isPushStateWhenCoordinator();
      assert csConf.getAsyncStoreConfig().getThreadPoolSize() == 5;
      assert csConf.getAsyncStoreConfig().getFlushLockTimeout() == 15000;
      assert csConf.getAsyncStoreConfig().isEnabled();
      assert csConf.getAsyncStoreConfig().getModificationQueueSize() == 700;

      c = getNamedCacheConfig(namedCaches, "withLoaderDefaults");
      csConf = (FileCacheStoreConfig) c.getCacheLoaders().get(0);
      assert csConf.getCacheLoaderClassName().equals("org.infinispan.loaders.file.FileCacheStore");
      assert csConf.getLocation().equals("/tmp/Another-FileCacheStore-Location");
      assert csConf.getFsyncMode() == FileCacheStoreConfig.FsyncMode.DEFAULT;

      c = getNamedCacheConfig(namedCaches, "withouthJmxEnabled");
      assert !c.isExposeJmxStatistics();
      assert !gc.isExposeGlobalJmxStatistics();
      assert gc.isAllowDuplicateDomains();
      assert gc.getJmxDomain().equals("funky_domain");
      assert gc.getMBeanServerLookup().equals("org.infinispan.jmx.PerThreadMBeanServerLookup");

      c = getNamedCacheConfig(namedCaches, "dist");
      assert c.getCacheMode() == Configuration.CacheMode.DIST_SYNC;
      assert c.getL1Lifespan() == 600000;
      assert c.getRehashWaitTime() == 120000;
      assert c.getConsistentHashClass().equals(TopologyAwareConsistentHash.class.getName());
      assert c.getNumOwners() == 3;
      assert c.isL1CacheEnabled();
      
      c = getNamedCacheConfig(namedCaches, "groups");
      Assert.assertTrue(c.isGroupsEnabled());
      Assert.assertEquals(c.getGroupers().size(), 1);
      Assert.assertEquals(c.getGroupers().get(0).getKeyType(), String.class);

      c = getNamedCacheConfig(namedCaches, "cacheWithCustomInterceptors");
      assert !c.getCustomInterceptors().isEmpty();
      assert c.getCustomInterceptors().size() == 5;

      c = getNamedCacheConfig(namedCaches, "evictionCache");
      assert c.getEvictionMaxEntries() == 5000;
      assert c.getEvictionStrategy().equals(EvictionStrategy.FIFO);
      assert c.getExpirationLifespan() == 60000;
      assert c.getExpirationMaxIdle() == 1000;
      assert c.getEvictionThreadPolicy() == EvictionThreadPolicy.PIGGYBACK;

      c = getNamedCacheConfig(namedCaches, "withDeadlockDetection");
      assert c.isEnableDeadlockDetection();
      assert c.getDeadlockDetectionSpinDuration() == 1221;
      assert c.getCacheMode() == Configuration.CacheMode.DIST_SYNC;
   }

   private Configuration getNamedCacheConfig(Map<String, Configuration> namedCaches, String cacheName) {
      Configuration c = namedCaches.get(cacheName);
      c.assertValid();
      return c;
   }

   private void testConfigurationMerging(XmlConfigurationParser parser) {

      Configuration defaultCfg = parser.parseDefaultConfiguration();
      Map<String, Configuration> namedCaches = parser.parseNamedConfigurations();

      Configuration c = defaultCfg.clone();
      c.applyOverrides(getNamedCacheConfig(namedCaches, "transactional"));

      assert c.getCacheMode() == Configuration.CacheMode.LOCAL;
      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getConcurrencyLevel() == 100;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;

      c = defaultCfg.clone();
      c.applyOverrides(getNamedCacheConfig(namedCaches, "syncRepl"));

      assert c.getTransactionManagerLookupClass() == null;
      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
      assert !c.isFetchInMemoryState();
      assert c.getSyncReplTimeout() == 15000;
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
      assert c.getConcurrencyLevel() == 100;

      c = defaultCfg.clone();
      c.applyOverrides(getNamedCacheConfig(namedCaches, "asyncRepl"));

      assert c.getTransactionManagerLookupClass() == null;
      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert !c.isUseReplQueue();
      assert !c.isUseAsyncMarshalling();
      assert !c.isFetchInMemoryState();
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
      assert c.getConcurrencyLevel() == 100;

      c = defaultCfg.clone();
      c.applyOverrides(getNamedCacheConfig(namedCaches, "asyncReplQueue"));

      assert c.getTransactionManagerLookupClass() == null;
      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert c.isUseReplQueue();
      assert c.getReplQueueInterval() == 1234;
      assert c.getReplQueueMaxElements() == 100;
      assert !c.isUseAsyncMarshalling();
      assert !c.isFetchInMemoryState();
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
      assert c.getConcurrencyLevel() == 100;

      c = defaultCfg.clone();
      c.applyOverrides(getNamedCacheConfig(namedCaches, "txSyncRepl"));
      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
      assert !c.isFetchInMemoryState();
      assert c.getSyncReplTimeout() == 15000;
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
      assert c.getConcurrencyLevel() == 100;

      c = defaultCfg.clone();
      c.applyOverrides(getNamedCacheConfig(namedCaches, "overriding"));

      assert c.getTransactionManagerLookupClass() == null;
      assert c.getCacheMode() == Configuration.CacheMode.LOCAL;
      assert c.getLockAcquisitionTimeout() == 20000;
      assert c.getConcurrencyLevel() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;

      c = defaultCfg.clone();
      c.applyOverrides(getNamedCacheConfig(namedCaches, "storeAsBinary"));
      assert c.isStoreAsBinary();
      assert !c.isExposeJmxStatistics();

      c = defaultCfg.clone();
      c.applyOverrides(getNamedCacheConfig(namedCaches, "withReplicationQueue"));
      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert c.isUseReplQueue();
      assert c.getReplQueueInterval() == 100;
      assert c.getReplQueueMaxElements() == 200;

      c = defaultCfg.clone();
      Configuration configurationWL = getNamedCacheConfig(namedCaches, "withLoader");
      configurationWL.getCacheLoaderManagerConfig().setShared(true);
      FileCacheStoreConfig clc = (FileCacheStoreConfig)configurationWL.getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
      clc.getSingletonStoreConfig().setPushStateTimeout(254L);
      clc.getAsyncStoreConfig().setThreadPoolSize(7);
      
      c.applyOverrides(configurationWL);     
      CacheLoaderManagerConfig loaderManagerConfig = c.getCacheLoaderManagerConfig();
      assert loaderManagerConfig.isPreload();
      assert !loaderManagerConfig.isPassivation();
      assert loaderManagerConfig.isShared();
      assert loaderManagerConfig.getCacheLoaderConfigs().size() == 1;
      FileCacheStoreConfig csConf = (FileCacheStoreConfig) loaderManagerConfig.getFirstCacheLoaderConfig();
      assert csConf.getCacheLoaderClassName().equals("org.infinispan.loaders.file.FileCacheStore");
      assert csConf.isFetchPersistentState();
      assert csConf.isIgnoreModifications();
      assert csConf.isPurgeOnStartup();
      assert csConf.getLocation().equals("/tmp/FileCacheStore-Location");
      assert csConf.getSingletonStoreConfig().getPushStateTimeout() == 254L;
      assert csConf.getSingletonStoreConfig().isPushStateWhenCoordinator();
      assert csConf.getAsyncStoreConfig().getThreadPoolSize() == 7;
      assert csConf.getAsyncStoreConfig().getFlushLockTimeout() == 15000;
      assert csConf.getAsyncStoreConfig().isEnabled();
   }

}