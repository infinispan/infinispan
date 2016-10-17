package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;

/**
 * @author wburns
 * @since 9.0
 */
public class LocalAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected LocalAutoCommitExpiryTest() {
      super(CacheMode.LOCAL, true);
   }
}
