package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "expiry.ReplNoAutoCommitExpiryTest")
public class ReplNoAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected ReplNoAutoCommitExpiryTest() {
      super(CacheMode.REPL_SYNC, false);
   }
}
