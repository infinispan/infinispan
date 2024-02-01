package org.infinispan.server.resp.dist;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.RespSingleNodeTest;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.SkipException;
import org.testng.annotations.Test;

import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "dist.server.resp.RespMultiNodeTest")
public class RespMultiNodeTest extends RespSingleNodeTest {

   private CacheMode mode;

   @Override
   public Object[] factory() {
      return new Object[] {
            new RespMultiNodeTest().withCacheMode(CacheMode.DIST_SYNC),
            new RespMultiNodeTest().withCacheMode(CacheMode.REPL_SYNC),
      };
   }

   protected RespMultiNodeTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
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

   @Override
   public void testClusterShardsSingleNode() {
      RedisCommands<String, String> redis = redisConnection.sync();
      switch (mode) {
         // In REPL, every node is listed as segment owner.
         case REPL_SYNC -> assertThat(redis.clusterShards()).hasSize(3);

         // In DIST, we have every combination of 3 nodes owning segments: AB, AC, BA, BC, CA, CB
         case DIST_SYNC -> assertThat(redis.clusterShards()).hasSize(6);
      }
   }

   @Override
   public void testClusterNodesSingleNode() {
      throw new SkipException("Not executed in clustered test");
   }
}
