package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "expiry.ReplAutoCommitExpiryTest")
public class ReplAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected ReplAutoCommitExpiryTest() {
      super(CacheMode.REPL_SYNC, true);
   }
}
