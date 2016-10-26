package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "expiry.DistAutoCommitExpiryTest")
public class DistAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected DistAutoCommitExpiryTest() {
      super(CacheMode.DIST_SYNC, true);
   }
}
