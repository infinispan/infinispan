package org.infinispan.distexec.mapreduce;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * LargeDistributedTwoNodesMapReduceTest tests large Map/Reduce functionality using two Infinispan
 * nodes, distributed reduce and individual per task intermediate key/value cache
 *
 * @author Vladimir Blagojevic
 * @since 7.0
 */
@Test(groups = "stress", testName = "distexec.mapreduce.LargeDistributedTwoNodesMapReduceTest")
public class LargeDistributedTwoNodesMapReduceTest extends BaseLargeWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      builder.clustering().stateTransfer().chunkSize(20).sync().replTimeout(45, TimeUnit.SECONDS);
      createClusteredCaches(2, cacheName(), builder);
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c) {
      //run distributed reduce with per task cache
      return new MapReduceTask<String, String, String, Integer>(c, true, false);
   }
}
