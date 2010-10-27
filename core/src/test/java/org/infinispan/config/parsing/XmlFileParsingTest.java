package org.infinispan.config.parsing;

import org.infinispan.Version;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationValidatingVisitor;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG_40;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG_NO_SCHEMA;

@Test(groups = "unit", testName = "config.parsing.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   public void testNamedCacheFileJaxb() throws Exception {
      String schemaFileName = "infinispan-config-" + Version.getMajorVersion() + ".xsd";
      testNamedCacheFile(InfinispanConfiguration.newInfinispanConfiguration(
            "configs/named-cache-test.xml", "schema/" + schemaFileName, new ConfigurationValidatingVisitor()));
   }

   public void testConfigurationMergingJaxb() throws Exception {
      testConfigurationMerging(InfinispanConfiguration
            .newInfinispanConfiguration("configs/named-cache-test.xml"));
   }

   public void testConfigSampleAllValidation() throws Exception {
      String schemaFileName = "infinispan-config-" + Version.getMajorVersion() + ".xsd";
      InfinispanConfiguration.newInfinispanConfiguration("config-samples/all.xml", "schema/" + schemaFileName, new ConfigurationValidatingVisitor());
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

      String schemaFileName = "schema/infinispan-config-" + Version.getMajorVersion() + ".xsd";
      
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

   private void testNamedCacheFile(XmlConfigurationParser parser) throws IOException {

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

      Configuration defaultConfiguration = parser.parseDefaultConfiguration();

      assert defaultConfiguration.getLockAcquisitionTimeout() == 1000;
      assert defaultConfiguration.getConcurrencyLevel() == 100;
      assert defaultConfiguration.getIsolationLevel() == IsolationLevel.READ_COMMITTED;

      Map<String, Configuration> namedCaches = parser.parseNamedConfigurations();

      Configuration c = namedCaches.get("transactional");

      assert !c.getCacheMode().isClustered();
      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
      assert c.isUseEagerLocking();
      assert c.isEagerLockSingleNode();
      assert !c.isSyncRollbackPhase();

      c = namedCaches.get("transactional2");
      assert c.getTransactionManagerLookupClass().equals("org.something.Lookup");

      c = namedCaches.get("syncRepl");

      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
      assert !c.isFetchInMemoryState();
      assert c.getSyncReplTimeout() == 15000;

      c = namedCaches.get("asyncRepl");

      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert !c.isUseReplQueue();
      assert !c.isUseAsyncMarshalling();
      assert !c.isFetchInMemoryState();

      c = namedCaches.get("asyncReplQueue");

      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert c.isUseReplQueue();
      assert !c.isUseAsyncMarshalling();
      assert !c.isFetchInMemoryState();

      c = namedCaches.get("txSyncRepl");

      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
      assert !c.isFetchInMemoryState();
      assert c.getSyncReplTimeout() == 15000;

      c = namedCaches.get("overriding");

      assert c.getTransactionManagerLookupClass() == null;
      assert c.getCacheMode() == Configuration.CacheMode.LOCAL;
      assert c.getLockAcquisitionTimeout() == 20000;
      assert c.getConcurrencyLevel() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;
      assert !c.isUseLazyDeserialization();

      c = namedCaches.get("lazyDeserialization");
      assert c.isUseLazyDeserialization();

      c = namedCaches.get("withLoader");
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
      assert csConf.getSingletonStoreConfig().getPushStateTimeout() == 20000;
      assert csConf.getSingletonStoreConfig().isPushStateWhenCoordinator();
      assert csConf.getAsyncStoreConfig().getThreadPoolSize() == 5;
      assert csConf.getAsyncStoreConfig().getFlushLockTimeout() == 15000;
      assert csConf.getAsyncStoreConfig().isEnabled();

      c = namedCaches.get("withouthJmxEnabled");
      assert !c.isExposeJmxStatistics();
      assert !gc.isExposeGlobalJmxStatistics();
      assert gc.isAllowDuplicateDomains();
      assert gc.getJmxDomain().equals("funky_domain");
      assert gc.getMBeanServerLookup().equals("org.infinispan.jmx.PerThreadMBeanServerLookup");

      c = namedCaches.get("dist");
      assert c.getCacheMode() == Configuration.CacheMode.DIST_SYNC;
      assert c.getL1Lifespan() == 600000;
      assert c.getRehashWaitTime() == 120000;
      assert c.getConsistentHashClass().equals(DefaultConsistentHash.class.getName());
      assert c.getNumOwners() == 3;
      assert c.isL1CacheEnabled();

      c = namedCaches.get("cacheWithCustomInterceptors");
      assert !c.getCustomInterceptors().isEmpty();
      assert c.getCustomInterceptors().size() == 5;

      c = namedCaches.get("evictionCache");
      assert c.getEvictionMaxEntries() == 5000;
      assert c.getEvictionStrategy().equals(EvictionStrategy.FIFO);
      assert c.getExpirationLifespan() == 60000;
      assert c.getExpirationMaxIdle() == 1000;
      assert c.getEvictionThreadPolicy() == EvictionThreadPolicy.PIGGYBACK;

      c = namedCaches.get("withDeadlockDetection");
      assert c.isEnableDeadlockDetection();
      assert c.getDeadlockDetectionSpinDuration() == 1221;
      assert c.getCacheMode() == Configuration.CacheMode.DIST_SYNC;
   }

   private void testConfigurationMerging(XmlConfigurationParser parser) throws IOException {

      Configuration defaultCfg = parser.parseDefaultConfiguration();
      Map<String, Configuration> namedCaches = parser.parseNamedConfigurations();

      Configuration c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("transactional"));

      assert c.getCacheMode() == Configuration.CacheMode.LOCAL;
      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getConcurrencyLevel() == 100;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;

      c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("syncRepl"));

      assert c.getTransactionManagerLookupClass() == null;
      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
      assert !c.isFetchInMemoryState();
      assert c.getSyncReplTimeout() == 15000;
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
      assert c.getConcurrencyLevel() == 100;

      c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("asyncRepl"));

      assert c.getTransactionManagerLookupClass() == null;
      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert !c.isUseReplQueue();
      assert !c.isUseAsyncMarshalling();
      assert !c.isFetchInMemoryState();
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
      assert c.getConcurrencyLevel() == 100;

      c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("asyncReplQueue"));

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
      c.applyOverrides(namedCaches.get("txSyncRepl"));
      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.lookup.GenericTransactionManagerLookup");
      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
      assert !c.isFetchInMemoryState();
      assert c.getSyncReplTimeout() == 15000;
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
      assert c.getConcurrencyLevel() == 100;

      c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("overriding"));

      assert c.getTransactionManagerLookupClass() == null;
      assert c.getCacheMode() == Configuration.CacheMode.LOCAL;
      assert c.getLockAcquisitionTimeout() == 20000;
      assert c.getConcurrencyLevel() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;

      c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("lazyDeserialization"));
      assert c.isUseLazyDeserialization();
      assert !c.isExposeJmxStatistics();

      c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("withReplicationQueue"));
      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert c.isUseReplQueue();
      assert c.getReplQueueInterval() == 100;
      assert c.getReplQueueMaxElements() == 200;

      c = defaultCfg.clone();
      Configuration configurationWL = namedCaches.get("withLoader");
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