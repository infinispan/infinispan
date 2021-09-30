package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@InCacheMode({CacheMode.DIST_SYNC, CacheMode.LOCAL})
@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreRestartTest")
public class SoftIndexFileStoreRestartTest extends BaseDistStoreTest<Integer, String, SoftIndexFileStoreRestartTest> {
   protected String tmpDirectory;

   {
      // We don't really need a cluster
      INIT_CLUSTER_SIZE = 1;
      l1CacheEnabled = false;
      segmented = true;
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
      super.createBeforeClass();
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected StoreConfigurationBuilder addStore(PersistenceConfigurationBuilder persistenceConfigurationBuilder, boolean shared) {
      // We don't support shared for SIFS
      assert !shared;
      return persistenceConfigurationBuilder.addSoftIndexFileStore()
            .dataLocation(CommonsTestingUtil.tmpDirectory(tmpDirectory, "data"))
            .indexLocation(CommonsTestingUtil.tmpDirectory(tmpDirectory, "index"));
   }

   public void testRestartWithNoIndex() throws Throwable {
      int size = 10;
      for (int i = 0; i < size; i++) {
         cache(0, cacheName).put(i, "value-" + i);
      }
      assertEquals(size, cache(0, cacheName).size());

      killMember(0, cacheName);

      // Delete the index which should force it to rebuild
      Util.recursiveFileRemove((CommonsTestingUtil.tmpDirectory(tmpDirectory, "index")));

      createCacheManagers();

      assertEquals(size, cache(0, cacheName).size());
      for (int i = 0; i < size; i++) {
         assertEquals("value-" + i, cache(0, cacheName).get(i));
      }
   }
}
