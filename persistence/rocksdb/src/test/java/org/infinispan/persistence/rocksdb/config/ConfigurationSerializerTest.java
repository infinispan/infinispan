package org.infinispan.persistence.rocksdb.config;

import static org.testng.Assert.assertEquals;

import java.nio.file.Paths;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "persistence.rocksdb.configuration.ConfigurationSerializerTest", groups="functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][] {
            {Paths.get("config/rocksdb-config.xml")},
      };
   }

   @Override
   protected void compareStoreConfiguration(String name, StoreConfiguration beforeStore, StoreConfiguration afterStore) {
      super.compareStoreConfiguration(name, beforeStore, afterStore);
      RocksDBStoreConfiguration before = (RocksDBStoreConfiguration) beforeStore;
      RocksDBStoreConfiguration after = (RocksDBStoreConfiguration) afterStore;
      assertEquals(before.attributes(), after.attributes());
      assertEquals(before.expiration().attributes(), after.expiration().attributes());
   }
}
