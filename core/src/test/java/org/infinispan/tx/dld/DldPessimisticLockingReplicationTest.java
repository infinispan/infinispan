package org.infinispan.tx.dld;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "tx.dld.DldPessimisticLockingReplicationTest")
public class DldPessimisticLockingReplicationTest extends BaseDldPessimisticLockingTest {
   protected ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configuration.transaction().lockingMode(LockingMode.PESSIMISTIC)
            .locking().useLockStriping(false)
            .deadlockDetection().enable();
      return configuration;
   }
}
