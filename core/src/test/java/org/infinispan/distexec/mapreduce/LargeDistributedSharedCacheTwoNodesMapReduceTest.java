package org.infinispan.distexec.mapreduce;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * LargeDistributedSharedCacheTwoNodesMapReduceTest tests large Map/Reduce functionality using two
 * Infinispan nodes, distributed reduce and shared intermediate key/value cache
 *
 * @author Vladimir Blagojevic
 * @since 7.0
 */
@Test(groups = "stress", testName = "distexec.mapreduce.LargeDistributedSharedCacheTwoNodesMapReduceTest")
public class LargeDistributedSharedCacheTwoNodesMapReduceTest extends BaseLargeWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      builder.clustering().stateTransfer().sync().replTimeout(45, TimeUnit.SECONDS);
      createClusteredCaches(2, cacheName(), builder);
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c){
      //run distributed reduce with intermediate shared cache
      return new MapReduceTask<String, String, String, Integer>(c, true);
   }
}
