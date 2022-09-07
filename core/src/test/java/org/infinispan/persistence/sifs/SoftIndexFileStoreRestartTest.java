package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;

import java.nio.file.Paths;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
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
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString());
   }

   public void testRestartWithNoIndex() throws Throwable {
      int size = 10;
      for (int i = 0; i < size; i++) {
         cache(0, cacheName).put(i, "value-" + i);
      }
      assertEquals(size, cache(0, cacheName).size());

      killMember(0, cacheName);

      // Delete the index which should force it to rebuild
      Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));

      createCacheManagers();

      assertEquals(size, cache(0, cacheName).size());
      for (int i = 0; i < size; i++) {
         assertEquals("value-" + i, cache(0, cacheName).get(i));
      }
   }


   @DataProvider(name = "booleans")
   Object[][] booleans() {
      return new Object[][]{
            {Boolean.TRUE}, {Boolean.FALSE}};
   }

   @Test(dataProvider = "booleans")
   public void testRestartWithEntryUpdatedMultipleTimes(boolean leafOrNode) throws Throwable {
      int size = 10;
      String key = "compaction";
      // We want to test both a leaf and node storage on the root node
      int extraInserts = leafOrNode ? size : size * 256;
      for (int i = 0; i < extraInserts; i++) {
         // Have some extra entries which prevent it from running compaction at beginning
         cache(0, cacheName).put(i, "value-" + i);
      }
      for (int i = 0; i < size; i++) {
         cache(0, cacheName).put(key, "value-" + i);
      }
      assertEquals(extraInserts + 1, cache(0, cacheName).size());

      killMember(0, cacheName);
      // NOTE: we keep the index, so we ensure upon restart that it is correct

      createCacheManagers();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // Force compaction for the previous file
      CompletionStages.join(compactor.forceCompactionForAllNonLogFiles());

      assertEquals("value-" + (size - 1), cache(0, cacheName).get(key));
   }
}
