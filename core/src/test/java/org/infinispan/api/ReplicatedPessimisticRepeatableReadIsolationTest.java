package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "api.ReplicatedPessimisticRepeatableReadIsolationTest")
public class ReplicatedPessimisticRepeatableReadIsolationTest extends AbstractRepeatableReadIsolationTest {

   public ReplicatedPessimisticRepeatableReadIsolationTest() {
      super(CacheMode.REPL_SYNC, LockingMode.PESSIMISTIC);
   }
}
