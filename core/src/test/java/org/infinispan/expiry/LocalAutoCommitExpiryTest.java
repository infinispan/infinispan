package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "expiry.LocalAutoCommitExpiryTest")
public class LocalAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected LocalAutoCommitExpiryTest() {
      super(CacheMode.LOCAL, true);
   }
}
