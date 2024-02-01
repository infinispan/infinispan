package org.infinispan.server.resp.dist;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.RespBxPOPTest;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "dist.server.resp.BlockingCommandsClusteredTest")
public class BlockingCommandsClusteredTest extends RespBxPOPTest {

   private CacheMode mode;

   private BlockingCommandsClusteredTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
   }

   @Override
   protected BlockingCommandsClusteredTest right() {
      super.right();
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new BlockingCommandsClusteredTest().withCacheMode(CacheMode.DIST_SYNC).right(),
            new BlockingCommandsClusteredTest().withCacheMode(CacheMode.DIST_SYNC),
            new BlockingCommandsClusteredTest().withCacheMode(CacheMode.REPL_SYNC),
            new BlockingCommandsClusteredTest().withCacheMode(CacheMode.REPL_SYNC).right(),
      };
   }

   @Override
   protected String parameters() {
      return "[right=" + isRight() + ", mode=" + mode + "]";
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
   public void testBxPopMultipleListenersTwoKeysTwoEvents() throws InterruptedException, ExecutionException, TimeoutException {
      super.testBxPopMultipleListenersTwoKeysTwoEvents();
   }
}
