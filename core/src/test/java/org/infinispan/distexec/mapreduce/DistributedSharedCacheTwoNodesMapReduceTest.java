package org.infinispan.distexec.mapreduce;

import static org.junit.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
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

   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[]) throws Exception{
      MapReduceTask<String,String,String,Integer> task = super.invokeMapReduce(keys, true);

      //Let's check that shared intermediate cache got cleaned up
      //See https://issues.jboss.org/browse/ISPN-4471 for more details
      EmbeddedCacheManager cacheManager = getCacheManagers().get(0);
      String sharedCache = task.getIntermediateCacheName();
      assertEquals("MapReduceTask has not use shared intermediate cache as it should have but it used " + sharedCache,
            MapReduceTask.DEFAULT_TMP_CACHE_CONFIGURATION_NAME, sharedCache);
      Cache<Object, Object> sharedTmpCache = cacheManager.getCache(sharedCache);
      int elementsInCache = sharedTmpCache.size();
      assertEquals("Shared cache " + sharedCache + " is not empty. It has " + elementsInCache + " keys/values: " +
                  sharedTmpCache.entrySet(), 0, elementsInCache);
      return task;
   }
}
