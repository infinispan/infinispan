package org.infinispan.notifications.cachelistener;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


// Since all tests currently are clustered only, there is no reason to even enable this test as nothing would work
@Test(groups = "unit", testName = "notifications.cachelistener.CacheNotifierImplInitialTransferInvalTest")
public class CacheNotifierImplInitialTransferInvalTest extends BaseCacheNotifierImplInitialTransferTest {
   protected CacheNotifierImplInitialTransferInvalTest() {
      super(CacheMode.INVALIDATION_SYNC);
   }

   @Override
   // This is disabled because invalidation caches don't work with cluster listeners
   @Test(enabled = false)
   public void testModificationAfterIterationBeganButNotIteratedValueYetClustered() {
   }

   @Override
   // This is disabled because invalidation caches don't work with cluster listeners
   @Test(enabled = false)
   public void testSimpleCacheStartingClusterListener() {
   }

   @Override
   // This is disabled because invalidation caches don't work with cluster listeners
   @Test(enabled = false)
   public void testModificationAfterIterationBeganAndIteratedValueClustered() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
   }

   @Override
   // This is disabled because invalidation caches don't work with cluster listeners
   @Test(enabled = false)
   public void testCreateAfterIterationBeganAndIteratedValueClustered() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
   }

   @Override
   // This is disabled because invalidation caches don't work with cluster listeners
   @Test(enabled = false)
   public void testCreateAfterIterationBeganButNotIteratedValueYetClustered() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
   }

   @Override
   // This is disabled because invalidation caches don't work with cluster listeners
   @Test(enabled = false)
   public void testRemoveAfterIterationBeganAndIteratedValueClustered() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
   }

   @Override
   // This is disabled because invalidation caches don't work with cluster listeners
   @Test(enabled = false)
   public void testRemoveAfterIterationBeganButNotIteratedValueYetClustered() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
   }

   @Override
   // This is disabled because invalidation caches don't work with cluster listeners
   @Test(enabled = false)
   public void testFilterConverterUnusedDuringIteration() {
   }

   @Override
   // This is disabled because invalidation caches don't work with cluster listeners
   @Test(enabled = false)
   public void testMetadataAvailable() {
   }
}
