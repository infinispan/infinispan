package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * ReplicatedFourNodesMapReduceTest tests Map/Reduce functionality using four Infinispan nodes,
 * replicated reduce and individual per task intermediate key/value cache
 *
 * @author wburns
 * @since 5.3
 */
@Test(groups = "functional", testName = "distexec.mapreduce.ReplicatedFourNodesMapReduceTest")
public class ReplicatedFourNodesMapReduceTest extends DistributedFourNodesMapReduceTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }
}
