package org.infinispan.api.lru.read_committed;

import org.infinispan.api.CacheAPITest;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.read_committed.CacheAPIMVCCTest")
public class CacheAPIMVCCTest extends CacheAPITest {
   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.READ_COMMITTED;
   }

   @Override
   protected Configuration addEviction(Configuration cfg) {
      cfg.setEvictionStrategy(EvictionStrategy.LRU);
      cfg.setEvictionWakeUpInterval(60000);
      cfg.setEvictionMaxEntries(1000);
      return cfg;
   }
}
