package org.infinispan.replication;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.awt.color.CMMException;

@Test(groups = "functional", testName = "replication.CustomReplQueueTest")
public class CustomReplQueueTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setCacheMode(Configuration.CacheMode.REPL_ASYNC);
      cfg.setUseReplQueue(true);
      cfg.setReplQueueClass(TestReplQueueClass.class.getName());
      EmbeddedCacheManager ecm = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault(), cfg);
      return ecm;
   }

   public void testReplQueueImplType() {
      ReplicationQueue rq = TestingUtil.extractComponent(cache, ReplicationQueue.class);
      assert rq instanceof TestReplQueueClass;              
   }

   public static class TestReplQueueClass extends ReplicationQueueImpl {
   }
}
