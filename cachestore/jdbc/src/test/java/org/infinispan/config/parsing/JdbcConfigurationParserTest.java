package org.infinispan.config.parsing;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.testng.annotations.Test;
import org.w3c.dom.Element;

@Test(groups = "unit", testName = "config.parsing.JdbcConfigurationParserTest")
public class JdbcConfigurationParserTest {
   public void testCacheLoaders() throws Exception {
      XmlConfigurationParserImpl parser = new XmlConfigurationParserImpl();
      String xml =
            "      <loaders passivation=\"true\" shared=\"true\" preload=\"true\">\n" +
                  "         <loader class=\"org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore\" fetchPersistentState=\"true\"\n" +
                  "                 ignoreModifications=\"true\" purgeOnStartup=\"true\">\n" +
                  "            <properties>\n" +
                  "               <property name=\"connectionFactoryClass\" value=\"org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory\"/>\n" +
                  "               <property name=\"connectionUrl\" value=\"jdbc://some-url\"/>\n" +
                  "               <property name=\"userName\" value=\"root\"/>\n" +
                  "               <property name=\"driverClass\" value=\"org.dbms.Driver\"/>\n" +
                  "               <property name=\"idColumnType\" value=\"VARCHAR2(256)\"/>\n" +
                  "               <property name=\"dataColumnType\" value=\"BLOB\"/>\n" +
                  "               <property name=\"dropTableOnExit\" value=\"true\"/>\n" +
                  "               <property name=\"createTableOnStart\" value=\"false\"/>\n" +
                  "            </properties>\n" +
                  "            <singletonStore enabled=\"true\" pushStateWhenCoordinator=\"true\" pushStateTimeout=\"20000\"/>\n" +
                  "            <async enabled=\"true\" batchSize=\"15\"/>\n" +
                  "         </loader>\n" +
                  "      </loaders>      ";
      Element e = XmlConfigHelper.stringToElement(xml);

      Configuration c = new Configuration();
      parser.configureCacheLoaders(e, c);

      CacheLoaderManagerConfig clc = c.getCacheLoaderManagerConfig();
      assert clc != null;
      assert clc.isFetchPersistentState();
      assert clc.isPassivation();
      assert clc.isShared();
      assert clc.isPreload();

      CacheStoreConfig iclc = (CacheStoreConfig) clc.getFirstCacheLoaderConfig();
      assert iclc.getCacheLoaderClassName().equals(JdbcStringBasedCacheStore.class.getName());
      assert iclc.getAsyncStoreConfig().isEnabled();
      assert iclc.getAsyncStoreConfig().getBatchSize() == 15;
      assert iclc.getAsyncStoreConfig().getPollWait() == 100;
      assert iclc.getAsyncStoreConfig().getQueueSize() == 10000;
      assert iclc.getAsyncStoreConfig().getThreadPoolSize() == 1;
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
