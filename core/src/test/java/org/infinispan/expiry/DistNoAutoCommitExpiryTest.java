package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;

/**
 * @author wburns
 * @since 9.0
 */
public class DistNoAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected DistNoAutoCommitExpiryTest() {
      super(CacheMode.DIST_SYNC, false);
   }
}
