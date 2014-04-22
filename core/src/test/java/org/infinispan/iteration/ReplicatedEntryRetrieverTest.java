package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

/**
 * Test to verify entry retriever behavior for a replicated cache
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.ReplicatedEntryRetrieverTest")
public class ReplicatedEntryRetrieverTest extends BaseClusteredEntryRetrieverTest {

   public ReplicatedEntryRetrieverTest() {
      super(false, CacheMode.REPL_SYNC);
   }

   @Override
   protected Object getKeyTiedToCache(Cache<?, ?> cache) {
      return new MagicKey(cache);
   }
}
