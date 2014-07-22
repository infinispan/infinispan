package org.infinispan.distexec.mapreduce;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * LargeDistributedFourNodesMapReduceTest tests large Map/Reduce functionality using two Infinispan
 * nodes, distributed reduce and individual per task intermediate key/value cache
 *
 * @author Vladimir Blagojevic
 * @since 7.0
 */
@Test(groups = "stress", testName = "distexec.mapreduce.LargeDistributedFourNodesMapReduceTest")
public class LargeDistributedFourNodesMapReduceTest extends BaseLargeWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      builder.clustering().stateTransfer().sync().replTimeout(45, TimeUnit.SECONDS);
      createClusteredCaches(4, cacheName(), builder);
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c) {
      //run distributed reduce with per task cache
      return new MapReduceTask<String, String, String, Integer>(c, true, false);
   }

   @Override
   public void testInvokeMapReduceOnAllKeys() throws Exception {
      MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null);
      log.debug("Read macbeth.txt and inserted keys into cache. Executing M/R task...");
      long start = System.currentTimeMillis();
      Map<String, Integer> mapReduce = task.execute();
      log.debugf("Task completed in %s ms", System.currentTimeMillis() - start);
      verifyResults(mapReduce);
   }
}
