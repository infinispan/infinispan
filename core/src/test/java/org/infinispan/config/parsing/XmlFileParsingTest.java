package org.infinispan.config.parsing;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.loader.file.FileCacheStoreConfig;
import org.infinispan.lock.IsolationLevel;
import org.infinispan.distribution.DefaultConsistentHash;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

@Test(groups = "unit", testName = "config.parsing.XmlFileParsingTest")
public class XmlFileParsingTest {

   public void testNamedCacheFile() throws IOException {
      XmlConfigurationParser parser = new XmlConfigurationParserImpl("configs/named-cache-test.xml");

      GlobalConfiguration gc = parser.parseGlobalConfiguration();

      assert gc.getAsyncListenerExecutorFactoryClass().equals("org.infinispan.executors.DefaultExecutorFactory");
      assert gc.getAsyncListenerExecutorProperties().getProperty("maxThreads").equals("5");
      assert gc.getAsyncListenerExecutorProperties().getProperty("threadNamePrefix").equals("AsyncListenerThread");

      assert gc.getAsyncSerializationExecutorFactoryClass().equals("org.infinispan.executors.DefaultExecutorFactory");
      assert gc.getAsyncSerializationExecutorProperties().getProperty("maxThreads").equals("25");
      assert gc.getAsyncSerializationExecutorProperties().getProperty("threadNamePrefix").equals("AsyncSerializationThread");

      assert gc.getEvictionScheduledExecutorFactoryClass().equals("org.infinispan.executors.DefaultScheduledExecutorFactory");
      assert gc.getEvictionScheduledExecutorProperties().getProperty("threadNamePrefix").equals("EvictionThread");

      assert gc.getReplicationQueueScheduledExecutorFactoryClass().equals("org.infinispan.executors.DefaultScheduledExecutorFactory");
      assert gc.getReplicationQueueScheduledExecutorProperties().getProperty("threadNamePrefix").equals("ReplicationQueueThread");

      assert gc.getTransportClass().equals("org.infinispan.remoting.transport.jgroups.JGroupsTransport");
      assert gc.getTransportProperties().isEmpty();

      assert gc.getMarshallerClass().equals("org.infinispan.marshall.VersionAwareMarshaller");
      assert gc.getMarshallVersionString().equals("1.0");
      assert gc.getObjectOutputStreamPoolSize() == 100;
      assert gc.getObjectInputStreamPoolSize() == 100;

      Configuration defaultConfiguration = parser.parseDefaultConfiguration();

      assert defaultConfiguration.getLockAcquisitionTimeout() == 1000;
      assert defaultConfiguration.getConcurrencyLevel() == 100;
      assert defaultConfiguration.getIsolationLevel() == IsolationLevel.READ_COMMITTED;

      Map<String, Configuration> namedCaches = parser.parseNamedConfigurations();

      Configuration c = namedCaches.get("transactional");

      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.GenericTransactionManagerLookup");

      c = namedCaches.get("syncRepl");

      assert c.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
      assert !c.isFetchInMemoryState();
      assert c.getSyncReplTimeout() == 15000;

      c = namedCaches.get("asyncRepl");

      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert !c.isUseReplQueue();
      assert !c.isUseAsyncSerialization();
      assert !c.isFetchInMemoryState();

      c = namedCaches.get("asyncReplQueue");

      assert c.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
      assert c.isUseReplQueue();
      assert c.isUseAsyncSerialization();
      assert !c.isFetchInMemoryState();

      c = namedCaches.get("txSyncRepl");

      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.GenericTransactionManagerLookup");
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
      assert loaderManagerConfig.getCacheLoaderConfigs().size() == 1;
      FileCacheStoreConfig csConf = (FileCacheStoreConfig) loaderManagerConfig.getFirstCacheLoaderConfig();
      assert csConf.getCacheLoaderClassName().equals("org.infinispan.loader.file.FileCacheStore");
      assert csConf.isFetchPersistentState();
      assert csConf.isIgnoreModifications();
      assert csConf.isPurgeOnStartup();
      assert csConf.getLocation().equals("/tmp/FileCacheStore-Location");

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
   }

   public void testConfigurationMerging() throws IOException {
      XmlConfigurationParser parser = new XmlConfigurationParserImpl("configs/named-cache-test.xml");
      Configuration defaultCfg = parser.parseDefaultConfiguration();
      Map<String, Configuration> namedCaches = parser.parseNamedConfigurations();

      Configuration c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("transactional"));

      assert c.getCacheMode() == Configuration.CacheMode.LOCAL;
      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.GenericTransactionManagerLookup");
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
      assert !c.isUseAsyncSerialization();
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
      assert c.isUseAsyncSerialization();
      assert !c.isFetchInMemoryState();
      assert c.getLockAcquisitionTimeout() == 1000;
      assert c.getIsolationLevel() == IsolationLevel.READ_COMMITTED;
      assert c.getConcurrencyLevel() == 100;

      c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("txSyncRepl"));
      assert c.getTransactionManagerLookupClass().equals("org.infinispan.transaction.GenericTransactionManagerLookup");
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
   }
}