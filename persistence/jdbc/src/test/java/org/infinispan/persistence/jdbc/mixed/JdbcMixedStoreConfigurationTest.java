package org.infinispan.persistence.jdbc.mixed;

import junit.framework.Assert;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tester class for {@link org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfiguration}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "persistence.jdbc.mixed.JdbcMixedStoreConfigurationTest")
public class JdbcMixedStoreConfigurationTest {
   private JdbcMixedStoreConfiguration config;
   private JdbcMixedStoreConfigurationBuilder storeBuilder;

   @BeforeMethod
   public void setUp() {
      storeBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(JdbcMixedStoreConfigurationBuilder.class);
      storeBuilder
            .simpleConnection()
               .connectionUrl("url")
               .driverClass("driver");
   }

   /**
    * Just take some random props and check their correctness.
    */
   public void simpleTest() {
      storeBuilder
            .binaryTable()
               .createOnStart(false)
               .dataColumnName("binary_dc")
               .dataColumnType("binary_dct")
            .stringTable()
               .createOnStart(true)
               .dataColumnName("strings_dc")
               .dataColumnType("strings_dct");

      config = storeBuilder.create();

      //some checks
      Assert.assertFalse(config.binaryTable().createOnStart());
      Assert.assertTrue(config.stringTable().createOnStart());
      Assert.assertEquals(config.binaryTable().dataColumnName(), "binary_dc");
      Assert.assertEquals(config.binaryTable().dataColumnType(), "binary_dct");
      Assert.assertEquals(config.stringTable().dataColumnName(), "strings_dc");
      Assert.assertEquals(config.stringTable().dataColumnType(), "strings_dct");
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testSameTableName() {
      storeBuilder
            .binaryTable().tableNamePrefix("failTable")
            .stringTable().tableNamePrefix("failTable");
      storeBuilder.validate();
   }

   public void testKey2StringMapper() {
      storeBuilder.key2StringMapper(DefaultTwoWayKey2StringMapper.class.getName());
      config = storeBuilder.create();
      Assert.assertEquals(config.key2StringMapper(), DefaultTwoWayKey2StringMapper.class.getName());
   }

   public void testConcurrencyLevel() {
      config = storeBuilder.create();
      Assert.assertEquals(2048, config.lockConcurrencyLevel());
      JdbcMixedStoreConfigurationBuilder storeBuilder2 = TestCacheManagerFactory.getDefaultCacheConfiguration
            (false)
            .persistence()
            .addStore(JdbcMixedStoreConfigurationBuilder.class)
               .read(config)
               .lockConcurrencyLevel(12);
      config = storeBuilder2.create();
      Assert.assertEquals(12, config.lockConcurrencyLevel());
   }

   public void voidTestLockAcquisitionTimeout() {
      config = storeBuilder.create();
      Assert.assertEquals(60000, config.lockAcquisitionTimeout());
      JdbcMixedStoreConfigurationBuilder storeBuilder2 = TestCacheManagerFactory.getDefaultCacheConfiguration
            (false)
            .persistence()
            .addStore(JdbcMixedStoreConfigurationBuilder.class)
               .read(config)
               .lockConcurrencyLevel(13);
      config = storeBuilder2.create();
      Assert.assertEquals(13, config.lockConcurrencyLevel());
   }

   public void testDatabaseConfiguration() {
      ConfigurationBuilder bld = new ConfigurationBuilder();
      bld.clustering().cacheMode(CacheMode.LOCAL)
            .persistence()
            .addStore(JdbcMixedStoreConfigurationBuilder.class).fetchPersistentState(false).purgeOnStartup(false)
            .databaseType(DatabaseType.MYSQL)
            .stringTable()
            .dropOnExit(false)
            .createOnStart(true)
            .tableNamePrefix("ISPN6Alpha2_STRING")
            .idColumnName("ID").idColumnType("VARCHAR(255)")
            .dataColumnName("DATA").dataColumnType("VARBINARY(1000)")
            .timestampColumnName("TIMESTAMP").timestampColumnType("BIGINT")
            .binaryTable()
            .dropOnExit(false)
            .createOnStart(true)
            .tableNamePrefix("ISPN6Alpha2_BINARY")
            .idColumnName("ID").idColumnType("VARCHAR(255)")
            .dataColumnName("DATA").dataColumnType("VARBINARY(1000)")
            .timestampColumnName("TIMESTAMP").timestampColumnType("BIGINT")
            .dataSource()
            .jndiUrl("java:jboss/datasources/ExampleDS");
      Configuration build = bld.build();
      JdbcMixedStoreConfiguration sc = (JdbcMixedStoreConfiguration) build.persistence().stores().get(0);
      Assert.assertEquals(sc.databaseType(), DatabaseType.MYSQL);
      Assert.assertEquals(sc.binaryTable().databaseType(), DatabaseType.MYSQL);
      Assert.assertEquals(sc.stringTable().databaseType(), DatabaseType.MYSQL);
   }
}
