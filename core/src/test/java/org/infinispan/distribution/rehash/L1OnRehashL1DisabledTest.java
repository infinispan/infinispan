package org.infinispan.distribution.rehash;

import org.testng.annotations.Test;

/**
 * Test removal of rebalanced keys on join when L1 cache is disabled.
 *
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 5.0
 */
@Test(groups = "functional", testName = "distribution.rehash.L1OnRehashL1DisabledTest")
public class L1OnRehashL1DisabledTest extends L1OnRehashTest {
   public L1OnRehashL1DisabledTest() {
      super();
      this.l1CacheEnabled = false;
      this.l1OnRehash = false;
   }
}
