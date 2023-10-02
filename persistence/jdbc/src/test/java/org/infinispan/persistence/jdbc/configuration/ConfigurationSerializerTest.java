package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfiguration;
import org.testng.annotations.Test;

@Test(testName = "persistence.jdbc.configuration.ConfigurationSerializerTest", groups = "functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {
   @Override
   protected void compareStoreConfiguration(String name, StoreConfiguration beforeStore, StoreConfiguration afterStore) {
      if (beforeStore instanceof AbstractJdbcStoreConfiguration) {
         AbstractJdbcStoreConfiguration before = (AbstractJdbcStoreConfiguration) beforeStore;
         AbstractJdbcStoreConfiguration after = (AbstractJdbcStoreConfiguration) afterStore;
         compareAttributeSets("Configuration " + name + " JDBC connection factory", before.connectionFactory().attributes(), after.connectionFactory().attributes(), "username", "password");
      }
      if (beforeStore instanceof JdbcStringBasedStoreConfiguration) {
         JdbcStringBasedStoreConfiguration before = (JdbcStringBasedStoreConfiguration) beforeStore;
         JdbcStringBasedStoreConfiguration after = (JdbcStringBasedStoreConfiguration) afterStore;
         compareAttributeSets("Configuration " + name + " table", before.table().attributes(), after.table().attributes());
         compareAttributeSets("Configuration " + name + " idColumn", before.table().idColumnConfiguration().attributes(), after.table().idColumnConfiguration().attributes());
         compareAttributeSets("Configuration " + name + " dataColumn", before.table().dataColumnConfiguration().attributes(), after.table().dataColumnConfiguration().attributes());
         compareAttributeSets("Configuration " + name + " timestampColumn", before.table().timeStampColumnConfiguration().attributes(), after.table().timeStampColumnConfiguration().attributes());
         compareAttributeSets("Configuration " + name + " segmentColumn", before.table().segmentColumnConfiguration().attributes(), after.table().segmentColumnConfiguration().attributes());
      }
      super.compareStoreConfiguration(name, beforeStore, afterStore);
   }
}
