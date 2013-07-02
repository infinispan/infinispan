package org.infinispan.loaders.hbase.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.hbase.HBaseCacheStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.hbase.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testHBaseCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(HBaseCacheStoreConfigurationBuilder.class)
         .autoCreateTable(false)
         .entryColumnFamily("ECF")
         .entryTable("ET")
         .entryValueField("EVF")
         .expirationColumnFamily("XCF")
         .expirationTable("XT")
         .expirationValueField("XVF")
         .hbaseZookeeperClientPort(4321)
         .hbaseZookeeperQuorumHost("myhost")
         .sharedTable(true)
      .fetchPersistentState(true).async().enable();
      Configuration configuration = b.build();
      HBaseCacheStoreConfiguration store = (HBaseCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert !store.autoCreateTable();
      assert store.entryColumnFamily().equals("ECF");
      assert store.entryTable().equals("ET");
      assert store.entryValueField().equals("EVF");
      assert store.expirationColumnFamily().equals("XCF");
      assert store.expirationTable().equals("XT");
      assert store.expirationValueField().equals("XVF");
      assert store.hbaseZookeeperQuorumHost().equals("myhost");
      assert store.hbaseZookeeperClientPort() == 4321;
      assert store.sharedTable();
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(HBaseCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      HBaseCacheStoreConfiguration store2 = (HBaseCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert !store2.autoCreateTable();
      assert store2.entryColumnFamily().equals("ECF");
      assert store2.entryTable().equals("ET");
      assert store2.entryValueField().equals("EVF");
      assert store2.expirationColumnFamily().equals("XCF");
      assert store2.expirationTable().equals("XT");
      assert store2.expirationValueField().equals("XVF");
      assert store2.hbaseZookeeperQuorumHost().equals("myhost");
      assert store2.hbaseZookeeperClientPort() == 4321;
      assert store2.sharedTable();
      assert store2.fetchPersistentState();
      assert store2.async().enabled();

      HBaseCacheStoreConfig legacy = store.adapt();
      assert !legacy.isAutoCreateTable();
      assert legacy.getEntryColumnFamily().equals("ECF");
      assert legacy.getEntryTable().equals("ET");
      assert legacy.getEntryValueField().equals("EVF");
      assert legacy.getExpirationColumnFamily().equals("XCF");
      assert legacy.getExpirationTable().equals("XT");
      assert legacy.getExpirationValueField().equals("XVF");
      assert legacy.getHbaseZookeeperPropertyClientPort() == 4321;
      assert legacy.isFetchPersistentState();
      assert legacy.getAsyncStoreConfig().isEnabled();
   }
}
