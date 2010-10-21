package org.infinispan.api.lru.read_committed;

import org.infinispan.api.CacheAPITest;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.lru.read_committed.CacheAPIMVCCTest")
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

   public void testRollbackAfterClear() throws Exception {
      String key = "key", value = "value";
      int size = 0;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      TestingUtil.getTransactionManager(cache).begin();
      cache.clear();
      assert cache.get(key) == null;
      size = 0;
      assert size == cache.size();
      assert size == cache.keySet().size();
      assert size == cache.values().size();
      assert size == cache.entrySet().size();
      TestingUtil.getTransactionManager(cache).rollback();

      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

}
