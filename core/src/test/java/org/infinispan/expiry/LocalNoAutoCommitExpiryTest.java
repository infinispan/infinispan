package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "expiry.LocalNoAutoCommitExpiryTest")
public class LocalNoAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected LocalNoAutoCommitExpiryTest() {
      super(CacheMode.LOCAL, false);
   }
}
