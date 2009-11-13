package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.jdbc.stringbased.PersonKey2StringMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tester class for {@link JdbcMixedCacheStoreConfig}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "loaders.jdbc.mixed.JdbcMixedCacheStoreConfigTest")
public class JdbcMixedCacheStoreConfigTest {
   private JdbcMixedCacheStoreConfig config;

   @BeforeMethod
   public void createConfig() {
      config = new JdbcMixedCacheStoreConfig();
   }

   /**
    * Just take some random props and check their corectness.
    */
   public void simpleTest() {
      config = new JdbcMixedCacheStoreConfig();
      config.setConnectionUrl("url");
      config.setCreateTableOnStartForBinary(false);
      config.setCreateTableOnStartForStrings(true);
      config.setDataColumnNameForBinary("binary_dc");
      config.setDataColumnNameForStrings("strings_dc");
      config.setDataColumnTypeForBinary("binary_dct");
      config.setDataColumnTypeForStrings("strings_dct");
      config.setDriverClass("driver");

      //some checks
      assert !config.getBinaryCacheStoreConfig().getTableManipulation().isCreateTableOnStart();
      assert config.getStringCacheStoreConfig().getTableManipulation().isCreateTableOnStart();
      assert config.getConnectionFactoryConfig().getDriverClass().equals("driver");
      assert config.getBinaryCacheStoreConfig().getTableManipulation().getDataColumnName().equals("binary_dc");
      assert config.getBinaryCacheStoreConfig().getTableManipulation().getDataColumnType().equals("binary_dct");
      assert config.getStringCacheStoreConfig().getTableManipulation().getDataColumnName().equals("strings_dc");
      assert config.getStringCacheStoreConfig().getTableManipulation().getDataColumnType().equals("strings_dct");
   }

   public void testSameTableName() {
      config.setTableNamePrefixForBinary("table");
      try {
         config.setTableNamePrefixForStrings("table");
         assert false : "expection expected as same table name is not allowed for both cache stores";
      } catch (Exception e) {
         //expected
      }
      //and the other way around
      config.setTableNamePrefixForStrings("table2");
      try {
         config.setTableNamePrefixForBinary("table2");
         assert false : "expection expected as same table name is not allowed for both cache stores";
      } catch (Exception e) {
         //expected
      }
   }

   public void testKey2StringMapper() {
      config.setKey2StringMapperClass(PersonKey2StringMapper.class.getName());
      assert config.getStringCacheStoreConfig().getKey2StringMapper().getClass().equals(PersonKey2StringMapper.class);
   }

   public void testConcurrencyLevel() {
      assert config.getStringCacheStoreConfig().getLockConcurrencyLevel() == LockSupportCacheStoreConfig.DEFAULT_CONCURRENCY_LEVEL / 2;
      assert config.getBinaryCacheStoreConfig().getLockConcurrencyLevel() == LockSupportCacheStoreConfig.DEFAULT_CONCURRENCY_LEVEL / 2;
      config.setLockConcurrencyLevelForStrings(11);
      config.setLockConcurrencyLevelForBinary(12);
      assert config.getStringCacheStoreConfig().getLockConcurrencyLevel() == 11;
      assert config.getBinaryCacheStoreConfig().getLockConcurrencyLevel() == 12;
   }

   public void testEnforcedSyncPurging() {
      assert config.getBinaryCacheStoreConfig().isPurgeSynchronously();
      assert config.getStringCacheStoreConfig().isPurgeSynchronously();
   }

   public void voidTestLockAquisitionTimeout() {
      assert config.getStringCacheStoreConfig().getLockAcquistionTimeout() == LockSupportCacheStoreConfig.DEFAULT_LOCK_ACQUISITION_TIMEOUT;
      assert config.getBinaryCacheStoreConfig().getLockAcquistionTimeout() == LockSupportCacheStoreConfig.DEFAULT_LOCK_ACQUISITION_TIMEOUT;
      config.setLockAcquistionTimeout(13);
      assert config.getStringCacheStoreConfig().getLockAcquistionTimeout() == 13;
      assert config.getBinaryCacheStoreConfig().getLockAcquistionTimeout() == 13;
   }
}
