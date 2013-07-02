package org.infinispan.distribution.rehash;

import org.testng.annotations.Test;

/**
 * Test removal of rebalanced keys on join when L1 cache is enabled but L1OnRehash is disabled.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "distribution.rehash.L1OnRehashDisabledTest")
public class L1OnRehashDisabledTest extends L1OnRehashTest {
   public L1OnRehashDisabledTest() {
      super();
      this.l1OnRehash = false;
   }
}
