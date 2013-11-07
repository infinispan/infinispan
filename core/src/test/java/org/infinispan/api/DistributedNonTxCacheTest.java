package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Data inconsistency can happen in non-transactional caches. the tests replicates this scenario: assuming N1 and N2 are
 * owners of key K. N2 is the primary owner
 * <p/>
 * <ul>
 *    <li>N1 tries to update K. it forwards the command to N2.</li>
 *    <li>N2 acquires the lock, and forwards back to N1 (that applies the modification).</li>
 *    <li>N2 releases the lock and replies to N1.</li>
 *    <li>N1 applies again the modification without the lock.</li>
 * </ul>
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "api.DistributedNonTxCacheTest")
public class DistributedNonTxCacheTest extends ReplicatedNonTxCacheTest {

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
