package org.infinispan.api.lru.read_committed;

import org.infinispan.api.CacheAPITest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.lru.read_committed.CacheAPIMVCCTest")
public class CacheAPIMVCCTest extends CacheAPITest {
   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.READ_COMMITTED;
   }

   @Override
   protected ConfigurationBuilder addEviction(ConfigurationBuilder cb) {
      cb
            .eviction()
               .strategy(EvictionStrategy.LRU)
               .maxEntries(1000)
            .expiration()
               .wakeUpInterval(60000);
      return cb;
   }
}
