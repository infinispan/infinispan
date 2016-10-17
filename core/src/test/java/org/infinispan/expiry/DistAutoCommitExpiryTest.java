package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;

/**
 * @author wburns
 * @since 9.0
 */
public class DistAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected DistAutoCommitExpiryTest() {
      super(CacheMode.DIST_SYNC, true);
   }
}
