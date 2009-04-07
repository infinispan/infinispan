package org.infinispan.loader.bdbje;

import org.infinispan.loader.CacheLoaderException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests that cover {@link  BdbjeCacheStoreConfig }
 *
 * @author Adrian Cole
 * @version $Id: $
 * @since 4.0
 */
@Test(groups = "unit", enabled = true, testName = "loader.bdbje.BdbjeCacheStoreConfigTest")
public class BdbjeCacheStoreConfigTest {

   private BdbjeCacheStoreConfig config;

   @BeforeMethod
   public void setUp() throws Exception {
      config = new BdbjeCacheStoreConfig();
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      config = null;
   }


   @Test
   public void testGetClassNameDefault() {
      assert config.getCacheLoaderClassName().equals(BdbjeCacheStore.class.getName());
   }

   @Test
   public void testgetMaxTxRetries() {
      assert config.getMaxTxRetries() == 5;
   }

   @Test
   public void testSetMaxTxRetries() {
      config.setMaxTxRetries(1);
      assert config.getMaxTxRetries() == 1;
   }

   @Test
   public void testGetLockAcquistionTimeout() {
      assert config.getLockAcquistionTimeout() == 60 * 1000;
   }

   @Test
   public void testSetLockAcquistionTimeoutMicros() {
      config.setLockAcquistionTimeout(1);
      assert config.getLockAcquistionTimeout() == 1;
   }

   @Test
   public void testGetLocationDefault() {
      assert config.getLocation().equals("Horizon-BdbjeCacheStore");
   }

   @Test
   public void testSetLocation() {
      config.setLocation("foo");
      assert config.getLocation().equals("foo");
   }

   @Test
   public void testSetCacheDb() {
      config.setCacheDbName("foo");
      assert config.getCacheDbName().equals("foo");
   }

   @Test
   public void testSetCatalogDb() {
      config.setCatalogDbName("foo");
      assert config.getCatalogDbName().equals("foo");
   }
   
}
