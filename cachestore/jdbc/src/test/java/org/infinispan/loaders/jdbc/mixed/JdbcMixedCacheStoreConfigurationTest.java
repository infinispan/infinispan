package org.infinispan.loaders.jdbc.mixed;

import junit.framework.Assert;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedCacheStoreConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedCacheStoreConfigurationBuilder;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tester class for {@link org.infinispan.loaders.jdbc.configuration.JdbcMixedCacheStoreConfiguration}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "loaders.jdbc.mixed.JdbcMixedCacheStoreConfigurationTest")
public class JdbcMixedCacheStoreConfigurationTest {
   private JdbcMixedCacheStoreConfiguration config;
   private JdbcMixedCacheStoreConfigurationBuilder storeBuilder;

   @BeforeMethod
   public void setUp() {
      storeBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false)
            .loaders()
               .addLoader(JdbcMixedCacheStoreConfigurationBuilder.class)
               .purgeSynchronously(true);
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
            .purgeSynchronously(true)
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
      JdbcMixedCacheStoreConfigurationBuilder storeBuilder2 = TestCacheManagerFactory.getDefaultCacheConfiguration
            (false)
            .loaders()
            .addLoader(JdbcMixedCacheStoreConfigurationBuilder.class)
               .read(config)
               .lockConcurrencyLevel(12);
      config = storeBuilder2.create();
      Assert.assertEquals(12, config.lockConcurrencyLevel());
   }

   public void testEnforcedSyncPurging() {
      config = storeBuilder.create();
      Assert.assertTrue(config.purgeSynchronously());
   }

   public void voidTestLockAcquisitionTimeout() {
      config = storeBuilder.create();
      Assert.assertEquals(60000, config.lockAcquistionTimeout());
      JdbcMixedCacheStoreConfigurationBuilder storeBuilder2 = TestCacheManagerFactory.getDefaultCacheConfiguration
            (false)
            .loaders()
            .addLoader(JdbcMixedCacheStoreConfigurationBuilder.class)
               .read(config)
               .lockConcurrencyLevel(13);
      config = storeBuilder2.create();
      Assert.assertEquals(13, config.lockConcurrencyLevel());
   }
}
