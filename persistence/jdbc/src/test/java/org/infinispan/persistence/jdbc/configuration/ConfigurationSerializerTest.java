package org.infinispan.persistence.jdbc.configuration;

import static org.testng.AssertJUnit.assertEquals;

import java.nio.file.Paths;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "persistence.jdbc.configuration.ConfigurationSerializerTest", groups = "functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][]{
            {Paths.get("configs/string-based.xml")}
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
      }
      super.compareStoreConfiguration(name, beforeStore, afterStore);
   }
}
