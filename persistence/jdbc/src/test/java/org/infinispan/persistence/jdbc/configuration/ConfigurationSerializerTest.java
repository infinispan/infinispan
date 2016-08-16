package org.infinispan.persistence.jdbc.configuration;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "persistence.jdbc.configuration.ConfigurationSerializerTest", groups = "functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][]{
            {"configs/binary.xml"},
            {"configs/mixed.xml"},
            {"configs/string-based.xml"}
      };
   }

   @Override
   protected void compareStoreConfiguration(String name, StoreConfiguration beforeStore, StoreConfiguration afterStore) {
      if (beforeStore instanceof AbstractJdbcStoreConfiguration) {
         AbstractJdbcStoreConfiguration before = (AbstractJdbcStoreConfiguration) beforeStore;
         AbstractJdbcStoreConfiguration after = (AbstractJdbcStoreConfiguration) afterStore;
         assertEquals("Configuration " + name + " JDBC connection factory", before.connectionFactory(), after.connectionFactory());
      }
      if (beforeStore instanceof JdbcStringBasedStoreConfiguration) {
         JdbcStringBasedStoreConfiguration before = (JdbcStringBasedStoreConfiguration) beforeStore;
         JdbcStringBasedStoreConfiguration after = (JdbcStringBasedStoreConfiguration) afterStore;
         compareAttributeSets("Configuration " + name + " table", before.table().attributes(), after.table().attributes());
      } else if (beforeStore instanceof JdbcBinaryStoreConfiguration) {
         JdbcBinaryStoreConfiguration before = (JdbcBinaryStoreConfiguration) beforeStore;
         JdbcBinaryStoreConfiguration after = (JdbcBinaryStoreConfiguration) afterStore;
         compareAttributeSets("Configuration " + name + " table", before.table().attributes(), after.table().attributes());
      } else if (beforeStore instanceof JdbcMixedStoreConfiguration) {
         JdbcMixedStoreConfiguration before = (JdbcMixedStoreConfiguration) beforeStore;
         JdbcMixedStoreConfiguration after = (JdbcMixedStoreConfiguration) afterStore;
         compareAttributeSets("Configuration " + name + " string table", before.stringTable().attributes(), after.stringTable().attributes());
         compareAttributeSets("Configuration " + name + " binary table", before.binaryTable().attributes(), after.binaryTable().attributes());
      }
      super.compareStoreConfiguration(name, beforeStore, afterStore);
   }
}
