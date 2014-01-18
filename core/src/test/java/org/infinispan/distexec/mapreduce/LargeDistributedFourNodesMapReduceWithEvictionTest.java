package org.infinispan.distexec.mapreduce;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * LargeDistributedFourNodesMapReduceWithEvictionTest tests large Map/Reduce functionality using
 * four Infinispan nodes, distributed reduce and individual per task intermediate key/value cache.
 *
 * Eviction is enabled in order to turn on BCHM backed DataContainer.
 *
 * @author Vladimir Blagojevic
 * @since 7.0
 */
@Test(groups = "stress", testName = "distexec.mapreduce.LargeDistributedFourNodesMapReduceWithEvictionTest")
public class LargeDistributedFourNodesMapReduceWithEvictionTest extends BaseLargeWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      builder.clustering().stateTransfer().chunkSize(20).sync().replTimeout(45, TimeUnit.SECONDS).eviction()
            .maxEntries(100000);
      createClusteredCaches(4, cacheName(), builder);
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c) {
      //run distributed reduce with per task cache
      return new MapReduceTask<String, String, String, Integer>(c, true, false);
   }
}
