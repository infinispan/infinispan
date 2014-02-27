package org.infinispan.distexec.mapreduce;


import java.util.Iterator;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;


/**
 * DistributedIntermediateCacheFourNodesMapReduceTest tests Map/Reduce functionality using four Infinispan nodes,
 * distributed reduce and individual per task intermediate key/value cache specified by application
 *
 * @author Vladimir Blagojevic
 * @since 7.0
 */
@Test(groups = "functional", testName = "distexec.mapreduce.DistributedIntermediateCacheFourNodesMapReduceTest")
public class DistributedIntermediateCacheFourNodesMapReduceTest extends BaseWordCountMapReduceTest {

   private String intermediateCacheNameConfig = "tmpCacheConfig";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(4, cacheName(), builder);
   }

   /**
    * We are overriding createMapReduceTask factory method from superclass so that all test methods
    * from superclass are run with application specified intermediate cache.
    *
    */
   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c) {
      //run distributed reduce with per task cache - cache specified by the user
      MapReduceTask<String, String, String, Integer> t = new MapReduceTask<String, String, String, Integer>(c, true,
            false);
      ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
      cacheConfig.unsafe().unreliableReturnValues(true)
      .clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2).sync();

      //In the real world people will define configurations using xml files
      defineConfigurationOnAllManagers(intermediateCacheNameConfig, cacheConfig);
      return t.usingIntermediateCache(intermediateCacheNameConfig);
   }
   
   @Test(expectedExceptions={CacheException.class})
   public void testIntermediateCacheNotCreatedOnAllNodes() throws Exception {
      String cacheNameConfig = "notCreatedOnAllNodes";
      Cache c = cache(0, cacheName());
      MapReduceTask<String, String, String, Integer> t = new MapReduceTask<String, String, String, Integer>(c, true,
            false);
      ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
      cacheConfig.unsafe().unreliableReturnValues(true).clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2)
            .sync();

      //define configuration only on first node
      Iterator<EmbeddedCacheManager> iterator = getCacheManagers().iterator();
      iterator.next().defineConfiguration(cacheNameConfig, cacheConfig.build());

      t.usingIntermediateCache(cacheNameConfig);
      t.mappedWith(new WordCountMapper()).reducedWith(new WordCountReducer());
      t.execute();
   }
}
