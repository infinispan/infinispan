package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;

import java.nio.file.Paths;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreSplitInnerNodeTest")
public class SoftIndexFileStoreSplitInnerNodeTest extends MultipleCacheManagersTest {
   protected String tmpDirectory;
   private final int MAX_ENTRIES = 597 + 1;
   private static final String CACHE_NAME = "SIFS-Backed";
   private Cache<Object, Object> c1;

   @BeforeSuite(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = Testing.tmpDirectory(getClass());
   }

   @AfterSuite(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected void createCacheManagers() {
      createCluster(2);

      for (EmbeddedCacheManager manager : managers()) {
         ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
         cb.clustering().hash().numSegments(1);
         cb.memory().maxCount((long) Math.ceil(MAX_ENTRIES * 0.2))
               .persistence().passivation(false)
               .addSoftIndexFileStore()
               .dataLocation(Paths.get(tmpDirectory, manager.getAddress().toString(), "data").toString())
               .indexLocation(Paths.get(tmpDirectory, manager.getAddress().toString(), "index").toString())
               .purgeOnStartup(true)
               .preload(false)
               .expiration().wakeUpInterval(Long.MAX_VALUE)
               .indexing().disable();

         TestingUtil.defineConfiguration(manager, CACHE_NAME, cb.build());
      }

      c1 = cache(0, CACHE_NAME);
   }

   /**
    * Reproducer for ISPN-13968.
    *
    * This will create 3 segments in the store. The keys `k367` and `k527` map to segment 0.
    * This store segment has two inner nodes. At first, we would try to search the inner node's leaf,
    * since we do not find the segment 0 in the leaf, we would skip the complete segment, ignoring some keys.
    *
    * We added a change to: if the segment is not present in the inner node's leaf, we try the next
    * one, if exists any.
    */
   public void testPopulatingAndQueryingSize() {
      for (int i = 0; i < MAX_ENTRIES; i++) {
         c1.put("k" + i, "v" + i);
      }

      assertEquals(MAX_ENTRIES, c1.size());
   }
}
