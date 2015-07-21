package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;


/**
 * Tests to make sure that entry retriever pays attention to partition status
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "partitionhandling.EntryRetrieverReplPartitionHandlingTest")
public class EntryRetrieverReplPartitionHandlingTest extends EntryRetrieverDistPartitionHandlingTest {
   public EntryRetrieverReplPartitionHandlingTest() {
      cacheMode = CacheMode.REPL_SYNC;
   }

   @Test(enabled = false)
   @Override
   public void testUsingIteratorButPartitionOccursBeforeRetrievingRemoteValues() throws InterruptedException {
      // This test is disabled since we don't remotely retrieve values
   }

   @Test(enabled = false)
   @Override
   public void testUsingIteratorButPartitionOccursAfterRetrievingRemoteValues() throws InterruptedException {
      // This test is disabled since we don't remotely retrieve values
   }

   @Override
   public void testRetrievalWhenPartitionIsDegradedButLocal() {
      Cache<MagicKey, String> cache0 = cache(0);
      cache0.put(new MagicKey(cache(1), cache(2)), "not-local");
      cache0.put(new MagicKey(cache(0), cache(1)), "local");

      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      partition(0).assertDegradedMode();

      try (CloseableIterator<Map.Entry<MagicKey, String>> iterator = Closeables.iterator(cache0.getAdvancedCache()
              .withFlags(Flag.CACHE_MODE_LOCAL).entrySet().stream())) {
         assertNotNull(iterator.next());
         assertNotNull(iterator.next());
         assertFalse(iterator.hasNext());
      }
   }
}
