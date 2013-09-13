package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * CompatModeDistributedTwoNodesMapReduceTest tests Map/Reduce functionality using two Infinispan nodes,
 * distributed reduce and individual per task intermediate key/value cache, in compatibility mode.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "distexec.mapreduce.CompatModeDistributedTwoNodesMapReduceTest")
public class CompatModeDistributedTwoNodesMapReduceTest extends BaseWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true, false);
      builder.compatibility().enable()
            .clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .stateTransfer().fetchInMemoryState(false)
            .transaction().syncCommitPhase(true).syncRollbackPhase(true)
            .cacheStopTimeout(0L);
      createClusteredCaches(2, cacheName(), builder);
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c){
      //run distributed reduce with per task cache
      return new MapReduceTask<String, String, String, Integer>(c, true, false);
   }
}
