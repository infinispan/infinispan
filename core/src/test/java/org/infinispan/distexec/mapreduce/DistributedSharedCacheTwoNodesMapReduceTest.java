package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;


/**
 * DistributedSharedCacheTwoNodesMapReduceTest tests Map/Reduce functionality using two Infinispan nodes,
 * distributed reduce and shared intermediate key/value cache
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@Test(groups = "functional", testName = "distexec.mapreduce.DistributedSharedCacheTwoNodesMapReduceTest")
public class DistributedSharedCacheTwoNodesMapReduceTest extends BaseWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(2, cacheName(), builder);
   }
   
   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c){
      //run distributed reduce with intermediate shared cache
      return new MapReduceTask<String, String, String, Integer>(c, true);
   }
}
