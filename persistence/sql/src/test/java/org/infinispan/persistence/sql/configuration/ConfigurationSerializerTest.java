package org.infinispan.persistence.sql.configuration;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfiguration;
import org.testng.annotations.Test;

@Test(testName = "persistence.sql.configuration.ConfigurationSerializerTest", groups = "functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @Override
   protected void compareStoreConfiguration(String name, StoreConfiguration beforeStore, StoreConfiguration afterStore) {
      if (beforeStore instanceof AbstractJdbcStoreConfiguration) {
         AbstractJdbcStoreConfiguration before = (AbstractJdbcStoreConfiguration) beforeStore;
         AbstractJdbcStoreConfiguration after = (AbstractJdbcStoreConfiguration) afterStore;
         compareAttributeSets("Configuration " + name + " JDBC connection factory", before.connectionFactory().attributes(), after.connectionFactory().attributes(), "username", "password");
      }
      if (beforeStore instanceof QueriesJdbcStoreConfiguration) {
         QueriesJdbcStoreConfiguration before = (QueriesJdbcStoreConfiguration) beforeStore;
         QueriesJdbcStoreConfiguration after = (QueriesJdbcStoreConfiguration) afterStore;
         compareAttributeSets("Configuration " + name + " schema", before.schema().attributes(), after.schema().attributes());
         compareAttributeSets("Configuration " + name + " queries", before.getQueriesJdbcConfiguration().attributes(), after.getQueriesJdbcConfiguration().attributes());
      } else if (beforeStore instanceof TableJdbcStoreConfiguration) {
         TableJdbcStoreConfiguration before = (TableJdbcStoreConfiguration) beforeStore;
         TableJdbcStoreConfiguration after = (TableJdbcStoreConfiguration) afterStore;
         compareAttributeSets("Configuration " + name + " schema", before.schema().attributes(), after.schema().attributes());
      }
      super.compareStoreConfiguration(name, beforeStore, afterStore);
   }
}
