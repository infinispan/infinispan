package org.infinispan.tx.lockreordering;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

import static org.infinispan.tx.lockreordering.LocalLockReorderingTest.generateKeys;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups="functional", testName = "tx.lockreordering.ReplicatedLockReorderingTest")
public class ReplicatedLockReorderingTest extends DistLockReorderingTest {

   public ReplicatedLockReorderingTest() {
      cacheMode = CacheMode.REPL_SYNC;
   }
}
