package org.infinispan.api.mvcc.read_committed;

import org.infinispan.api.BaseCacheAPIPessimisticTest;
import org.infinispan.api.CacheAPITest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "api.mvcc.read_committed.CacheAPIPessimisticTest")
public class CacheAPIPessimisticTest extends BaseCacheAPIPessimisticTest {
   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.READ_COMMITTED;
   }
}
