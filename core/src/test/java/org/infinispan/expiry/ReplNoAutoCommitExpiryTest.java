package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;

/**
 * @author wburns
 * @since 9.0
 */
public class ReplNoAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected ReplNoAutoCommitExpiryTest() {
      super(CacheMode.REPL_SYNC, false);
   }
}
