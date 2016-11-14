package org.infinispan.persistence.jdbc.configuration;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Properties;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.jdbc.configuration.ConfigurationTest")
public class ConfigurationTest {

   private static final String JDBC_URL = "jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1";

   public void testImplicitPooledConnectionFactory() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
         .connectionPool().connectionUrl(JDBC_URL);
      Configuration configuration = b.build();
      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) configuration.persistence().stores().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
   }

   public void testImplicitManagedConnectionFactory() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
         .dataSource().jndiUrl("java:jboss/datasources/ExampleDS");
      Configuration configuration = b.build();
      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) configuration.persistence().stores().get(0);
      assert store.connectionFactory() instanceof ManagedConnectionFactoryConfiguration;
   }

   public void testJdbcStringCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
         .connectionPool()
            .connectionUrl(JDBC_URL)
         .fetchPersistentState(true)
         .table()
            .tableNamePrefix("STRINGS_")
            .idColumnName("id").idColumnType("VARCHAR")
            .dataColumnName("datum").dataColumnType("BINARY")
            .timestampColumnName("version").timestampColumnType("BIGINT")
         .async().enable();
      Configuration configuration = b.build();
      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) configuration.persistence().stores().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store.table().tableNamePrefix().equals("STRINGS_");
      assert store.table().idColumnName().equals("id");
      assert store.table().idColumnType().equals("VARCHAR");
      assert store.table().dataColumnName().equals("datum");
      assert store.table().dataColumnType().equals("BINARY");
      assert store.table().timestampColumnName().equals("version");
      assert store.table().timestampColumnType().equals("BIGINT");
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbcStringBasedStoreConfiguration store2 = (JdbcStringBasedStoreConfiguration) configuration2.persistence().stores().get(0);
      assert store2.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
      assert ((PooledConnectionFactoryConfiguration)store2.connectionFactory()).connectionUrl().equals(JDBC_URL);
      assert store2.table().tableNamePrefix().equals("STRINGS_");
      assert store2.table().idColumnName().equals("id");
      assert store2.table().idColumnType().equals("VARCHAR");
      assert store2.table().dataColumnName().equals("datum");
      assert store2.table().dataColumnType().equals("BINARY");
      assert store2.table().timestampColumnName().equals("version");
      assert store2.table().timestampColumnType().equals("BIGINT");
      assert store2.fetchPersistentState();
      assert store2.async().enabled();
   }

   public void testTableProperties() {
      Properties props = new Properties();
      props.put("createOnStart", "false");
      props.put("dropOnExit", "true");

      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
         .connectionPool().connectionUrl(JDBC_URL)
         .withProperties(props);
      Configuration stringConfiguration = b.build();

      JdbcStringBasedStoreConfiguration stringStoreConfiguration = (JdbcStringBasedStoreConfiguration) stringConfiguration.persistence().stores().get(0);
      assertFalse(stringStoreConfiguration.table().createOnStart());
      assertTrue(stringStoreConfiguration.table().dropOnExit());
   }
}
