package org.infinispan.api.mvcc.repeatable_read;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "api.mvcc.repeatable_read.CacheAPIPessimisticTest")
public class CacheAPIPessimisticTest extends CacheAPIOptimisticTest {
   @Override
   protected void amend(ConfigurationBuilder cb) {
      cb.transaction().lockingMode(LockingMode.PESSIMISTIC);
   }
}
