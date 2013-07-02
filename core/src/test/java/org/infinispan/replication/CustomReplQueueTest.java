package org.infinispan.replication;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "replication.CustomReplQueueTest")
public class CustomReplQueueTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(CacheMode.REPL_ASYNC)
            .async().useReplQueue(true).replQueue(new TestReplQueueClass());
      return TestCacheManagerFactory.createClusteredCacheManager(cfg);
   }

   public void testReplQueueImplType() {
      ReplicationQueue rq = TestingUtil.extractComponent(cache, ReplicationQueue.class);
      assert rq instanceof TestReplQueueClass;              
   }

   public static class TestReplQueueClass extends ReplicationQueueImpl {
   }
}
