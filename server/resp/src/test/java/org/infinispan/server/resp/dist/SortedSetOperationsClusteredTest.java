package org.infinispan.server.resp.dist;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.SortedSetCommandsTest;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "dist.server.resp.SortedSetOperationsClusteredTest")
public class SortedSetOperationsClusteredTest extends SortedSetCommandsTest {

   private CacheMode mode;

   private SortedSetOperationsClusteredTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new SortedSetOperationsClusteredTest().withCacheMode(CacheMode.DIST_SYNC),
            new SortedSetOperationsClusteredTest().withCacheMode(CacheMode.REPL_SYNC),
      };
   }

   @Override
   protected String parameters() {
      return "[mode=" + mode + "]";
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      configurationBuilder.clustering().cacheMode(mode);
   }

   @Override
   protected TestSetup setup() {
      return TestSetup.clusteredTestSetup(3);
   }
}
