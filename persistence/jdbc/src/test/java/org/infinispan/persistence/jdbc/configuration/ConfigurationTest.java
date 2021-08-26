package org.infinispan.persistence.jdbc.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Properties;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.common.configuration.ManagedConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfiguration;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.jdbc.configuration.ConfigurationTest")
public class ConfigurationTest {

   private static final String JDBC_URL = "jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1";

   public void testImplicitPooledConnectionFactory() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      JdbcStringBasedStoreConfigurationBuilder jdbc = b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.buildTableManipulation(jdbc.table());
      jdbc
         .connectionPool().connectionUrl(JDBC_URL);
      Configuration configuration = b.build();
      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) configuration.persistence().stores().get(0);
      assert store.connectionFactory() instanceof PooledConnectionFactoryConfiguration;
   }

   public void testImplicitManagedConnectionFactory() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      JdbcStringBasedStoreConfigurationBuilder jdbc = b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.buildTableManipulation(jdbc.table());
      jdbc.dataSource().jndiUrl("java:jboss/datasources/ExampleDS");
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
            .segmentColumnName("segfault").segmentColumnType("BIGINT")
         .async().enable();
      Configuration configuration = b.build();
      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) configuration.persistence().stores().get(0);
      assertTrue(store.connectionFactory() instanceof PooledConnectionFactoryConfiguration);
      assertEquals(JDBC_URL, ((PooledConnectionFactoryConfiguration) store.connectionFactory()).connectionUrl());
      assertEquals("STRINGS_", store.table().tableNamePrefix());
      assertEquals("id", store.table().idColumnName());
      assertEquals("VARCHAR", store.table().idColumnType());
      assertEquals("datum", store.table().dataColumnName());
      assertEquals("BINARY", store.table().dataColumnType());
      assertEquals("version", store.table().timestampColumnName());
      assertEquals("BIGINT", store.table().timestampColumnType());
      assertEquals("segfault", store.table().segmentColumnName());
      assertEquals("BIGINT", store.table().segmentColumnType());
      assertTrue(store.fetchPersistentState());
      assertTrue(store.async().enabled());

      b = new ConfigurationBuilder();
      b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbcStringBasedStoreConfiguration store2 = (JdbcStringBasedStoreConfiguration) configuration2.persistence().stores().get(0);
      assertTrue(store2.connectionFactory() instanceof PooledConnectionFactoryConfiguration);
      assertEquals(JDBC_URL, ((PooledConnectionFactoryConfiguration) store2.connectionFactory()).connectionUrl());
      assertEquals("STRINGS_", store2.table().tableNamePrefix());
      assertEquals("id", store2.table().idColumnName());
      assertEquals("VARCHAR", store2.table().idColumnType());
      assertEquals("datum", store2.table().dataColumnName());
      assertEquals("BINARY", store2.table().dataColumnType());
      assertEquals("version", store2.table().timestampColumnName());
      assertEquals("BIGINT", store2.table().timestampColumnType());
      assertEquals("segfault", store2.table().segmentColumnName());
      assertEquals("BIGINT", store2.table().segmentColumnType());
      assertTrue(store2.fetchPersistentState());
      assertTrue(store2.async().enabled());
   }

   public void testTableProperties() {
      Properties props = new Properties();
      props.put("createOnStart", "false");
      props.put("dropOnExit", "true");

      ConfigurationBuilder b = new ConfigurationBuilder();
      JdbcStringBasedStoreConfigurationBuilder jdbc = b.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.buildTableManipulation(jdbc.table());
      jdbc
         .connectionPool().connectionUrl(JDBC_URL)
         .withProperties(props);
      Configuration stringConfiguration = b.build();

      JdbcStringBasedStoreConfiguration stringStoreConfiguration = (JdbcStringBasedStoreConfiguration) stringConfiguration.persistence().stores().get(0);
      assertFalse(stringStoreConfiguration.table().createOnStart());
      assertTrue(stringStoreConfiguration.table().dropOnExit());
   }
}
