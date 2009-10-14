package org.infinispan.config.parsing;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.testng.annotations.Test;

import java.util.Map;

@Test(groups = "unit", testName = "config.parsing.JdbcConfigurationParserTest")
public class JdbcConfigurationParserTest {
   
   public void testCacheLoaders() throws Exception {
   
      InfinispanConfiguration configuration = InfinispanConfiguration.newInfinispanConfiguration("configs/jdbc-parsing-test.xml");
      Map<String, Configuration> namedConfigurations = configuration.parseNamedConfigurations();
      Configuration c = namedConfigurations.get("withJDBCLoader");
      CacheLoaderManagerConfig clc = c.getCacheLoaderManagerConfig();
      assert clc != null;
      assert clc.isFetchPersistentState();
      assert clc.isPassivation();
      assert clc.isShared();
      assert clc.isPreload();

      CacheStoreConfig iclc = (CacheStoreConfig) clc.getFirstCacheLoaderConfig();
      assert iclc.getCacheLoaderClassName().equals(JdbcStringBasedCacheStore.class.getName());
      assert iclc.getAsyncStoreConfig().isEnabled();
      assert iclc.getAsyncStoreConfig().getMapLockTimeout() == 10000;
      assert iclc.getAsyncStoreConfig().getThreadPoolSize() == 10;
      assert iclc.isFetchPersistentState();
      assert iclc.isIgnoreModifications();
      assert iclc.isPurgeOnStartup();

      assert clc.getCacheLoaderConfigs().size() == 1;
      JdbcStringBasedCacheStoreConfig csConf = (JdbcStringBasedCacheStoreConfig) clc.getFirstCacheLoaderConfig();
      assert csConf.getCacheLoaderClassName().equals("org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore");
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


      SingletonStoreConfig ssc = iclc.getSingletonStoreConfig();
      assert ssc.isSingletonStoreEnabled();
      assert ssc.isPushStateWhenCoordinator();
      assert ssc.getPushStateTimeout() == 20000;
   }
}
