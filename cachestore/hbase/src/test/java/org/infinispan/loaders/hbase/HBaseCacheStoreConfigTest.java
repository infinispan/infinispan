package org.infinispan.loaders.hbase;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.keymappers.MarshalledValueOrPrimitiveMapper;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.hbase.HBaseCacheStoreConfigTest")
public class HBaseCacheStoreConfigTest {

   public void setConfigurationPropertiesFileTest() throws CacheLoaderException {
      HBaseCacheStoreConfig config = new HBaseCacheStoreConfig();

      assert config.entryTable == "ISPNCacheStore";
      assert config.hbaseZookeeperQuorum == "localhost";
      assert config.hbaseZookeeperPropertyClientPort == 2181;
      assert config.entryColumnFamily == "E";
      assert config.entryValueField == "EV";
      assert config.expirationTable == "ISPNCacheStoreExpiration";
      assert config.expirationColumnFamily == "X";
      assert config.expirationValueField == "XV";
      assert config.autoCreateTable;
      assert !config.sharedTable;
      assert config.keyMapper == MarshalledValueOrPrimitiveMapper.class.getName();
   }

}
