package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "expiry.DistNoAutoCommitExpiryTest")
public class DistNoAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected DistNoAutoCommitExpiryTest() {
      super(CacheMode.DIST_SYNC, false);
   }
}
