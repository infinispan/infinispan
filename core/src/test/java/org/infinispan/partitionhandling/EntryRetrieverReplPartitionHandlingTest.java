package org.infinispan.partitionhandling;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;


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
   public void testUsingIteratorButPartitionOccursBeforeRetrievingRemoteValues() throws InterruptedException {
      // This test is disabled since we don't remotely retrieve values
   }
}
