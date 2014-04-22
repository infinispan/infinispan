package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test to verify entry retriever behavior for a local cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.InvalidationEntryRetrieverTest")
public class InvalidationEntryRetrieverTest extends BaseEntryRetrieverTest {
   public InvalidationEntryRetrieverTest() {
      super(false, CacheMode.INVALIDATION_SYNC);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }

      builderUsed.clustering().hash().numOwners(2);
      builderUsed.clustering().stateTransfer().chunkSize(50);
      createClusteredCaches(1, CACHE_NAME, builderUsed);
   }

   protected final AtomicInteger counter = new AtomicInteger();

   @Override
   protected Object getKeyTiedToCache(Cache<?, ?> cache) {
      return cache.toString() + "-" + counter.getAndIncrement();
   }
}
