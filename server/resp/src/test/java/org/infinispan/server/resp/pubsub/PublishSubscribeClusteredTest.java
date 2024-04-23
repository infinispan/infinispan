package org.infinispan.server.resp.pubsub;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "dist.server.resp.PublishSubscribeClusteredTest")
public class PublishSubscribeClusteredTest extends PublishSubscribeTest {

   private PublishSubscribeClusteredTest withCacheMode(CacheMode mode) {
      this.cacheMode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new PublishSubscribeClusteredTest().withCacheMode(CacheMode.DIST_SYNC),
            new PublishSubscribeClusteredTest().withCacheMode(CacheMode.REPL_SYNC),
      };
   }

   @Override
   protected String parameters() {
      return "[mode=" + cacheMode + "]";
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      configurationBuilder.clustering().cacheMode(cacheMode);
   }

   @Override
   protected TestSetup setup() {
      return TestSetup.clusteredTestSetup(3);
   }
}
