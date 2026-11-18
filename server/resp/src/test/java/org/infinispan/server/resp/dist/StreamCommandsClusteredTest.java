package org.infinispan.server.resp.dist;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.StreamCommandsTest;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.Test;

/**
 * Verifies the stream commands in a clustered environment: the bucket, the functions and their results are
 * marshalled between the cluster members.
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "dist.server.resp.StreamCommandsClusteredTest")
public class StreamCommandsClusteredTest extends StreamCommandsTest {

   private CacheMode mode;

   private StreamCommandsClusteredTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new StreamCommandsClusteredTest().withCacheMode(CacheMode.DIST_SYNC),
            new StreamCommandsClusteredTest().withCacheMode(CacheMode.REPL_SYNC),
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
