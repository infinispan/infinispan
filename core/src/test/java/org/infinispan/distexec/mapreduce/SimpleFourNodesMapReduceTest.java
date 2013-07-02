package org.infinispan.distexec.mapreduce;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * SimpleFourNodesMapReduceTest tests Map/Reduce functionality using four Infinispan nodes and local
 * reduce
 * 
 * @author Vladimir Blagojevic
 * @since 5.0
 */
@Test(groups = "functional", testName = "distexec.mapreduce.SimpleFourNodesMapReduceTest")
public class SimpleFourNodesMapReduceTest extends BaseWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(4, cacheName(), builder);
   }
}
