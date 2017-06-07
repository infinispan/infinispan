package org.infinispan.api.mvcc.read_committed;

import org.infinispan.api.BaseCacheAPIOptimisticTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.read_committed.CacheAPIOptimisticTest")
public class CacheAPIOptimisticTest extends BaseCacheAPIOptimisticTest {
   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.READ_COMMITTED;
   }
}
