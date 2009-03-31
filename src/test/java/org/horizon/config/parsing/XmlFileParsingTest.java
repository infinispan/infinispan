package org.horizon.config.parsing;

import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.loader.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.horizon.loader.jdbc.connectionfactory.PooledConnectionFactory;
import org.horizon.loader.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.horizon.loader.jdbc.TableManipulation;
import org.horizon.lock.IsolationLevel;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

@Test(groups = "unit", testName = "config.parsing.XmlFileParsingTest")
public class XmlFileParsingTest {
   public void testNamedCacheFile() throws IOException {
      XmlConfigurationParser parser = new XmlConfigurationParserImpl("configs/named-cache-test.xml");

      GlobalConfiguration gc = parser.parseGlobalConfiguration();

      assert gc.getAsyncListenerExecutorFactoryClass().equals("org.horizon.executors.DefaultExecutorFactory");
      assert gc.getAsyncListenerExecutorProperties().getProperty("maxThreads").equals("5");
      assert gc.getAsyncListenerExecutorProperties().getProperty("threadNamePrefix").equals("AsyncListenerThread");

      assert gc.getAsyncSerializationExecutorFactoryClass().equals("org.horizon.executors.DefaultExecutorFactory");
      assert gc.getAsyncSerializationExecutorProperties().getProperty("maxThreads").equals("25");
      assert gc.getAsyncSerializationExecutorProperties().getProperty("threadNamePrefix").equals("AsyncSerializationThread");

      assert gc.getEvictionScheduledExecutorFactoryClass().equals("org.horizon.executors.DefaultScheduledExecutorFactory");
      assert gc.getEvictionScheduledExecutorProperties().getProperty("threadNamePrefix").equals("EvictionThread");

      assert gc.getReplicationQueueScheduledExecutorFactoryClass().equals("org.horizon.executors.DefaultScheduledExecutorFactory");
      assert gc.getReplicationQueueScheduledExecutorProperties().getProperty("threadNamePrefix").equals("ReplicationQueueThread");

      assert gc.getTransportClass().equals("org.horizon.remoting.transport.jgroups.JGroupsTransport");
      assert gc.getTransportProperties().isEmpty();

      assert gc.getMarshallerClass().equals("org.horizon.marshall.VersionAwareMarshaller");
      assert gc.getMarshallVersionString().equals("1.0");
      assert gc.getObjectOutputStreamPoolSize() == 100;
      assert gc.getObjectInputStreamPoolSize() == 100;

      Configuration defaultConfiguration = parser.parseDefaultConfiguration();

      assert defaultConfiguration.getLockAcquisitionTimeout() == 1000;
      assert defaultConfiguration.getConcurrencyLevel() == 100;
      assert defaultConfiguration.getIsolationLevel() == IsolationLevel.READ_COMMITTED;

      Map<String, Configuration> namedCaches = parser.parseNamedConfigurations();

      Configuration c = namedCaches.get("transactional");

      assert c.getTransactionManagerLookupClass().equals("org.horizon.transaction.GenericTransactionManagerLookup");

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

      assert c.getTransactionManagerLookupClass().equals("org.horizon.transaction.GenericTransactionManagerLookup");
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
      JdbcStringBasedCacheStoreConfig csConf = (JdbcStringBasedCacheStoreConfig) loaderManagerConfig.getFirstCacheLoaderConfig();
      assert csConf.getCacheLoaderClassName().equals("org.horizon.loader.jdbc.stringbased.JdbcStringBasedCacheStore");
      assert csConf.isFetchPersistentState();
      assert csConf.isIgnoreModifications();
      assert csConf.isPurgeOnStartup();
      TableManipulation tableManipulation = csConf.getTableManipulation();
      ConnectionFactoryConfig cfc = csConf.getConnectionFactoryConfig();
      assert cfc.getConnectionFactoryClass().equals(PooledConnectionFactory.class.getName());
      assert cfc.getConnectionUrl().equals("jdbc://some-url");
      assert cfc.getUserName().equals("root");
      assert cfc.getDriverClass().equals("org.dbms.Driver");
      assert tableManipulation.getIdColumnType().equals("VARCHAR2(256)");
      assert tableManipulation.getDataColumnType().equals("BLOB");
      assert tableManipulation.isDropTableOnExit();
      assert !tableManipulation.isCreateTableOnStart();

      c = namedCaches.get("withouthJmxEnabled");
      assert !c.isExposeJmxStatistics();
      assert !gc.isExposeGlobalJmxStatistics();
      assert gc.isAllowDuplicateDomains();
      assert gc.getJmxDomain().equals("funky_domain");
      assert gc.getMBeanServerLookup().equals("org.horizon.jmx.PerThreadMBeanServerLookup");
   }

   public void testConfigurationMerging() throws IOException {
      XmlConfigurationParser parser = new XmlConfigurationParserImpl("configs/named-cache-test.xml");
      Configuration defaultCfg = parser.parseDefaultConfiguration();
      Map<String, Configuration> namedCaches = parser.parseNamedConfigurations();

      Configuration c = defaultCfg.clone();
      c.applyOverrides(namedCaches.get("transactional"));

      assert c.getCacheMode() == Configuration.CacheMode.LOCAL;
      assert c.getTransactionManagerLookupClass().equals("org.horizon.transaction.GenericTransactionManagerLookup");
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
      assert c.getTransactionManagerLookupClass().equals("org.horizon.transaction.GenericTransactionManagerLookup");
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
