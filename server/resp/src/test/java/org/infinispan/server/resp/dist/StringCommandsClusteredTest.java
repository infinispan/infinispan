package org.infinispan.server.resp.dist;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.StringCommandsTest;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;


@Test(groups = "functional", testName = "dist.server.resp.StringCommandsClusteredTest")
public class StringCommandsClusteredTest extends StringCommandsTest {

   private CacheMode mode;

   private StringCommandsClusteredTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new StringCommandsClusteredTest().withCacheMode(CacheMode.DIST_SYNC),
            new StringCommandsClusteredTest().withCacheMode(CacheMode.REPL_SYNC),
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

   @Override
   @Ignore("Checking for the existing type costs ~20% in write throughput. Make this a no-op for now")
   public void testSetWrongType() {

   }

}
