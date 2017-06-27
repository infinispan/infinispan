package org.infinispan.stream;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Test to verify stream behavior for an invalidation cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "stream.InvalidationStreamIteratorTest")
public class InvalidationStreamIteratorTest extends BaseStreamIteratorTest {
   public InvalidationStreamIteratorTest() {
      super(false, CacheMode.INVALIDATION_SYNC);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (transactional) {
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
