package org.infinispan.query.continuous;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Tests continuous query with multiple caches in replicated mode.
 *
 * @author vjuranek
 * @since 8.0
 */
@Test(groups = "functional", testName = "query.continuous.ContinuousQueryMultipleCachesReplTest")
public class ContinuousQueryMultipleCachesReplTest extends AbstractCQMultipleCachesTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

}
