package org.infinispan.server.resp.dist;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.StreamBlockingTest;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.Test;

/**
 * Verifies the blocking XREAD in a clustered environment: the blocked client is served with the data written by a
 * different cluster member.
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "dist.server.resp.StreamBlockingClusteredTest")
public class StreamBlockingClusteredTest extends StreamBlockingTest {

   private CacheMode mode;

   private StreamBlockingClusteredTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new StreamBlockingClusteredTest().withCacheMode(CacheMode.DIST_SYNC),
            new StreamBlockingClusteredTest().withCacheMode(CacheMode.REPL_SYNC),
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
