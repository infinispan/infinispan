package org.infinispan.expiry;

import org.infinispan.configuration.cache.CacheMode;

/**
 * @author wburns
 * @since 9.0
 */
public class ReplAutoCommitExpiryTest extends AutoCommitExpiryTest {
   protected ReplAutoCommitExpiryTest() {
      super(CacheMode.REPL_SYNC, true);
   }
}
