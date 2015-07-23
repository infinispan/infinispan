package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

/**
 * Test to verify stream behavior for a replicated cache
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.ReplicatedStreamIteratorTest")
public class ReplicatedStreamIteratorTest extends BaseClusteredStreamIteratorTest {

   public ReplicatedStreamIteratorTest() {
      super(false, CacheMode.REPL_SYNC);
   }

   @Override
   protected Object getKeyTiedToCache(Cache<?, ?> cache) {
      return new MagicKey(cache);
   }
}
