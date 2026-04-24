package org.infinispan.persistence.sifs;

import static org.infinispan.testing.Testing.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreCompactionAfterSegmentTransferTest")
@CleanupAfterMethod
public class SoftIndexFileStoreCompactionAfterSegmentTransferTest extends MultipleCacheManagersTest {
   private static final String CACHE_NAME = "testCache";
   private static final int NUM_SEGMENTS = 4;
   private static final int TOTAL_ENTRIES = 120;

   private ControlledConsistentHashFactory.Default consistentHashFactory;

   @Override
   protected void createCacheManagers() throws Throwable {
      // Node 0 owns all 4 segments so that its SIFS data files contain entries from all segments.
      // When segment 0 is later removed, only ~25% of entries in each data file are freed,
      // which stays below the compaction threshold and prevents automatic compaction of those files.
      consistentHashFactory = new ControlledConsistentHashFactory.Default(
            new int[][]{{0, 1}, {0, 2}, {0, 1}, {0, 2}});

      for (int i = 0; i < 3; i++) {
         createStatefulCacheManager("node-" + i);
      }
      waitForClusterToForm(CACHE_NAME);
   }

   private void createStatefulCacheManager(String id) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      Util.recursiveFileRemove(stateDirectory);

      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      global.serialization().addContextInitializer(ControlledConsistentHashFactory.SCI.INSTANCE);
      global.globalState().enabled(true)
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(stateDirectory);

      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      config.clustering().hash().numOwners(2).numSegments(NUM_SEGMENTS);
      config.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(consistentHashFactory);
      config.memory().maxCount(10);

      config.persistence().addSoftIndexFileStore()
            .maxFileSize(1000)
            .dataLocation(Paths.get(stateDirectory, "data").toString())
            .indexLocation(Paths.get(stateDirectory, "index").toString());

      EmbeddedCacheManager manager = addClusterEnabledCacheManager(global, null);
      manager.defineConfiguration(CACHE_NAME, config.build());
   }

   public void testCompactionAfterSegmentOwnershipChange() throws Exception {
      Cache<Integer, String> cache0 = cache(0, CACHE_NAME);
      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache0, KeyPartitioner.class);

      // Find a target key in segment 0 and collect segment-0 filler keys
      int targetKey = -1;
      List<Integer> segment0FillerKeys = new ArrayList<>();
      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         Object storageKey = cache0.getAdvancedCache().getKeyDataConversion().toStorage(i);
         if (keyPartitioner.getSegment(storageKey) == 0) {
            if (targetKey == -1) {
               targetKey = i;
            } else {
               segment0FillerKeys.add(i);
            }
         }
      }

      // Write all entries in key order so that segments are naturally interleaved
      // across SIFS data files. This ensures each file contains entries from multiple
      // segments, preventing any single file from being 100% freed when segment 0
      // is removed.
      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         cache0.put(i, "value-" + i);
      }

      // Add node 3: segment 0 moves to {node3, node1}, removing node 0 as owner.
      // Node 0's SIFS store will call removeSegments for segment 0, freeing ~25% of
      // entries in each data file but not enough to trigger automatic compaction.
      consistentHashFactory.setOwnerIndexes(
            new int[][]{{3, 1}, {0, 2}, {0, 1}, {0, 2}});
      createStatefulCacheManager("node-3");
      waitForClusterToForm(CACHE_NAME);

      // Remove node 3: segment 0 reverts to {node0, node1}.
      // State transfer writes segment 0 entries back to node 0's SIFS store,
      // creating new index entries with numRecords=1.
      consistentHashFactory.setOwnerIndexes(
            new int[][]{{0, 1}, {0, 2}, {0, 1}, {0, 2}});
      killMember(3, CACHE_NAME);

      // Just an extra modification - to ensure the data is read properly
      assertEquals("value-" + targetKey, cache0.remove(targetKey));
      assertNull(cache0.put(targetKey, "value-" + targetKey + "-2"));

      // Write filler keys (not the target key) with new values in a loop to cause
      // the file containing the state-transferred target entry to be compacted
      cache0 = cache(0, CACHE_NAME);
      for (int iter = 0; iter < 30; iter++) {
         for (int key : segment0FillerKeys) {
            cache0.put(key, "updated-" + iter + "-" + key);
         }
      }

      assertEquals("value-" + targetKey + "-2", cache0.get(targetKey));

      // Force compaction of all completed files. When the old pre-removal files are
      // compacted, their segment 0 entries (now pointing to new state-transferred
      // locations in the index) will generate DROPPED index requests that decrement
      // numRecords for the state-transferred entries.
      WaitDelegatingNonBlockingStore<Integer, String> store = TestingUtil.getFirstStoreWait(cache0);
      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");
      compactor.forceCompactionForAllNonLogFiles()
            .toCompletableFuture().get(10, TimeUnit.SECONDS);

      // Verify target entry still exists
      assertEquals("value-" + targetKey + "-2", cache0.get(targetKey));
      // Clear the data container to ensure the next get hits the store
      cache0.getAdvancedCache().getDataContainer().clear();
      assertEquals("value-" + targetKey + "-2", cache0.get(targetKey));
   }
}
