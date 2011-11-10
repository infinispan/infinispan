package org.infinispan.tx.lockreordering;

import org.infinispan.config.Configuration;
import org.testng.annotations.Test;

import static org.infinispan.tx.lockreordering.LocalLockReorderingTest.generateKeys;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups="functional", testName = "tx.lockreordering.ReplLockReorderingTest")
public class ReplicatedLockReorderingTest extends DistLockReorderingTest {

   public ReplicatedLockReorderingTest() {
      cacheMode = Configuration.CacheMode.REPL_SYNC;
   }

   @Override
   void buildKeys() {
      keys = generateKeys();
   }
}
