package org.infinispan.query.continuous;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Tests continuous query with multiple caches in distribute mode.
 *
 * @author vjuranek
 * @since 8.0
 */
@Test(groups = "functional", testName = "query.continuous.ContinuousQueryMultipleCachesDistTest")
public class ContinuousQueryMultipleCachesDistTest extends AbstractCQMultipleCachesTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }
   
}
