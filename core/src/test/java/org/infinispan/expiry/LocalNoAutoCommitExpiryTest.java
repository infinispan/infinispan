package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;

/**
 * @author wburns
 * @since 9.0
 */
public class LocalNoAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected LocalNoAutoCommitExpiryTest() {
      super(CacheMode.LOCAL, false);
   }
}
