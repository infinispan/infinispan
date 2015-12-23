package org.infinispan.tx.locking;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.locking.OptimisticDistTxTest")
public class OptimisticDistTxTest extends OptimisticReplTxTest {
   public OptimisticDistTxTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
   }
}
