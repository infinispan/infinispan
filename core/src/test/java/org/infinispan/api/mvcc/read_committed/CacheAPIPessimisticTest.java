package org.infinispan.api.mvcc.read_committed;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "api.mvcc.read_committed.CacheAPIPessimisticTest")
public class CacheAPIPessimisticTest extends CacheAPIOptimisticTest {
   @Override
   protected void amend(ConfigurationBuilder cb) {
      cb.transaction().lockingMode(LockingMode.PESSIMISTIC);
   }
}
